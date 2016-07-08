package postgres

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tracer/tracer"
	"github.com/tracer/tracer/server"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/opentracing/opentracing-go"
)

func init() {
	server.RegisterStorage("postgres", setup)
}

func setup(conf map[string]interface{}) (server.Storage, error) {
	url, ok := conf["url"].(string)
	if !ok {
		return nil, errors.New("missing url for postgres backend")
	}
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("drror connecting to PostgreSQL database: %s", err)
	}
	return New(db), nil
}

var _ server.Storage = (*Storage)(nil)

// timeRange represents a PostgreSQL tstzrange. Caveat: it only
// supports inclusive ranges.
type timeRange struct {
	Start time.Time
	End   time.Time
}

func (t *timeRange) Scan(src interface{}) error {
	const layout = "2006-01-02 15:04:05.999999-07"

	b := src.([]byte)
	b = b[2:]
	idx := bytes.IndexByte(b, '"')
	t1, err := time.Parse(layout, string(b[:idx]))
	if err != nil {
		return err
	}

	b = b[idx+1:]
	idx = bytes.IndexByte(b, '"')
	b = b[idx+1:]
	idx = bytes.IndexByte(b, '"')
	t2, err := time.Parse(layout, string(string(b[:idx])))
	if err != nil {
		return err
	}
	t.Start = t1
	t.End = t2
	return nil
}

func (t timeRange) Value() (driver.Value, error) {
	const layout = "2006-01-02 15:04:05.999999-07"
	return []byte(fmt.Sprintf(`["%s","%s"]`, t.Start.Format(layout), t.End.Format(layout))), nil
}

type Storage struct {
	db *sqlx.DB
}

func New(db *sql.DB) *Storage {
	return &Storage{db: sqlx.NewDb(db, "postgres")}
}

func (st *Storage) Store(sp tracer.RawSpan) (err error) {
	const upsertSpan = `
INSERT INTO spans (id, trace_id, time, service_name, operation_name)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (id) DO
  UPDATE SET
    time = $3,
    service_name = $4,
    operation_name = $5`
	const insertTag = `INSERT INTO tags (span_id, trace_id, key, value) VALUES ($1, $2, $3, $4)`
	const insertLog = `INSERT INTO tags (span_id, trace_id, key, value, time) VALUES ($1, $2, $3, $4, $5)`
	const insertParentRelation = `INSERT INTO relations (span1_id, span2_id, kind) VALUES ($1, $2, 'parent')`
	const insertParentSpan = `INSERT INTO spans (id, trace_id, time, service_name, operation_name) VALUES ($1, $2, $3, '', '') ON CONFLICT (id) DO NOTHING`

	tx, err := st.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	_, err = tx.Exec(upsertSpan,
		int64(sp.SpanID), int64(sp.TraceID), timeRange{sp.StartTime, sp.FinishTime}, sp.ServiceName, sp.OperationName)
	if err != nil {
		return err
	}

	if sp.ParentID != 0 {
		_, err = tx.Exec(insertParentSpan,
			int64(sp.ParentID), int64(sp.TraceID), timeRange{time.Time{}, time.Time{}})
		if err != nil {
			return err
		}
		_, err = tx.Exec(insertParentSpan,
			int64(sp.TraceID), int64(sp.TraceID), timeRange{sp.StartTime, sp.FinishTime})
		if err != nil {
			return err
		}

		_, err = tx.Exec(insertParentRelation,
			int64(sp.ParentID), int64(sp.SpanID))
		if err != nil {
			return err
		}
	}

	for k, v := range sp.Tags {
		vs := ""
		if v != nil {
			vs = fmt.Sprintf("%v", v)
		}
		_, err = tx.Exec(insertTag,
			int64(sp.SpanID), int64(sp.TraceID), k, vs)
		if err != nil {
			return err
		}
	}
	for _, l := range sp.Logs {
		v := ""
		if l.Payload != nil {
			v = fmt.Sprintf("%v", l.Payload)
		}
		_, err = tx.Exec(insertLog,
			int64(sp.SpanID), int64(sp.TraceID), l.Event, v, l.Timestamp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *Storage) TraceByID(id uint64) (tracer.RawTrace, error) {
	tx, err := st.db.Begin()
	if err != nil {
		return tracer.RawTrace{}, err
	}
	defer tx.Rollback()
	return st.traceByID(tx, id)
}

func (st *Storage) traceByID(tx *sql.Tx, id uint64) (tracer.RawTrace, error) {
	const selectTrace = `
SELECT spans.id, spans.trace_id, spans.time, spans.service_name, spans.operation_name, tags.key, tags.value, tags.time
FROM spans
  LEFT JOIN tags
    ON spans.id = tags.span_id
WHERE spans.trace_id = $1
ORDER BY
  spans.time ASC,
  spans.id`
	const selectRelations = `
SELECT r.span1_id, r.span2_id, r.kind
FROM relations AS r
JOIN spans ON spans.id = r.span1_id
WHERE spans.trace_id = $1;
`
	rows, err := tx.Query(selectTrace, int64(id))
	if err != nil {
		return tracer.RawTrace{}, err
	}
	spans, err := scanSpans(rows)
	if err != nil {
		return tracer.RawTrace{}, err
	}
	rows.Close()

	rows, err = tx.Query(selectRelations, int64(id))
	if err != nil {
		return tracer.RawTrace{}, err
	}
	var rels []tracer.RawRelation
	var parent, child int64
	var kind string
	for rows.Next() {
		if err := rows.Scan(&parent, &child, &kind); err != nil {
			return tracer.RawTrace{}, err
		}
		rels = append(rels, tracer.RawRelation{
			ParentID: uint64(parent),
			ChildID:  uint64(child),
			Kind:     kind,
		})
	}
	if err := rows.Err(); err != nil {
		return tracer.RawTrace{}, err
	}
	return tracer.RawTrace{
		TraceID:   id,
		Spans:     spans,
		Relations: rels,
	}, nil
}

func scanSpans(rows *sql.Rows) ([]tracer.RawSpan, error) {
	// TODO select parents
	var spans []tracer.RawSpan
	var (
		prevSpanID int64

		spanID        int64
		traceID       int64
		spanTime      timeRange
		serviceName   string
		operationName string
		tagKey        string
		tagValue      string
		tagTime       *time.Time
	)
	tagTime = new(time.Time)
	var span tracer.RawSpan
	for rows.Next() {
		if err := rows.Scan(&spanID, &traceID, &spanTime, &serviceName, &operationName, &tagKey, &tagValue, &tagTime); err != nil {
			return nil, err
		}
		if spanID != prevSpanID {
			if prevSpanID != 0 {
				spans = append(spans, span)
			}
			prevSpanID = spanID
			span = tracer.RawSpan{
				Tags: map[string]interface{}{},
			}
		}
		span.SpanID = uint64(spanID)
		span.TraceID = uint64(traceID)
		span.StartTime = spanTime.Start
		span.FinishTime = spanTime.End
		span.ServiceName = serviceName
		span.OperationName = operationName
		if tagKey != "" {
			if tagTime == nil {
				span.Tags[tagKey] = tagValue
			} else {
				span.Logs = append(span.Logs, opentracing.LogData{
					Timestamp: *tagTime,
					Event:     tagKey,
					Payload:   tagValue,
				})
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if span.SpanID != 0 {
		spans = append(spans, span)
	}
	return spans, nil
}

func (st *Storage) SpanByID(id uint64) (tracer.RawSpan, error) {
	tx, err := st.db.Begin()
	if err != nil {
		return tracer.RawSpan{}, err
	}
	defer tx.Rollback()
	return st.spanByID(tx, id)
}

func (st *Storage) spanByID(tx *sql.Tx, id uint64) (tracer.RawSpan, error) {
	const selectSpan = `
SELECT spans.id, spans.trace_id, spans.time, spans.service_name, spans.operation_name, tags.key, tags.value, tags.time
FROM spans
  LEFT JOIN tags
    ON spans.id = tags.span_id
WHERE spans.id = $1
LIMIT 1`
	rows, err := tx.Query(selectSpan, int64(id))
	if err != nil {
		return tracer.RawSpan{}, err
	}
	defer rows.Close()
	spans, err := scanSpans(rows)
	if err != nil {
		return tracer.RawSpan{}, err
	}
	if len(spans) == 0 {
		return tracer.RawSpan{}, sql.ErrNoRows
	}
	return spans[0], nil
}

func (st *Storage) QueryTraces(q server.Query) ([]tracer.RawTrace, error) {
	tx, err := st.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var andConds []string
	var andArgs []interface{}
	var orConds []string
	var orArgs []interface{}
	if q.FinishTime.IsZero() {
		q.FinishTime = time.Now()
	}
	if q.MaxDuration == 0 {
		q.MaxDuration = 1<<31 - 1
	}
	for _, tag := range q.AndTags {
		if tag.CheckValue {
			andConds = append(andConds, `(tags.key = ? AND tags.value = ?)`)
			andArgs = append(andArgs, tag.Key, tag.Value)
		} else {
			andConds = append(andConds, `(tags.key = ?)`)
			andArgs = append(andArgs, tag.Key)
		}
	}

	for _, tag := range q.OrTags {
		if tag.CheckValue {
			orConds = append(orConds, `(tags.key = ? AND tags.value = ?)`)
			orArgs = append(orArgs, tag.Key, tag.Value)
		} else {
			orConds = append(orConds, `(tags.key = ?)`)
			orArgs = append(orArgs, tag.Key)
		}
	}

	and := strings.Join(andConds, " AND ")
	or := strings.Join(orConds, " OR ")
	conds := []string{"true"}
	if and != "" {
		conds = append(conds, and)
	}
	if or != "" {
		conds = append(conds, or)
	}

	query := st.db.Rebind(`
SELECT spans.trace_id
FROM spans
WHERE
  EXISTS (
    SELECT 1
    FROM tags
    WHERE
      tags.trace_id = spans.trace_id AND
      ` + strings.Join(conds, " AND ") + `
  ) AND
  ? @> spans.time AND
  (? = '' OR operation_name = ?) AND
  DURATION(time) >= ? AND
  DURATION(time) <= ? AND
  spans.id = spans.trace_id
ORDER BY
  spans.time ASC,
  spans.trace_id
`)
	args := make([]interface{}, 0, len(andArgs)+len(orArgs))
	args = append(args, andArgs...)
	args = append(args, orArgs...)
	args = append(args, timeRange{q.StartTime, q.FinishTime})
	args = append(args, q.OperationName, q.OperationName)
	args = append(args, int64(q.MinDuration), int64(q.MaxDuration))

	var ids []int64
	rows, err := st.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var id int64
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var traces []tracer.RawTrace
	for _, id := range ids {
		trace, err := st.traceByID(tx, uint64(id))
		if err != nil {
			return nil, err
		}
		traces = append(traces, trace)
	}
	return traces, nil
}
