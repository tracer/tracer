package postgres

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"honnef.co/go/tracer"

	"github.com/jmoiron/sqlx"
	"github.com/opentracing/opentracing-go"
)

type Storage struct {
	db *sqlx.DB
}

func New(db *sql.DB) *Storage {
	return &Storage{db: sqlx.NewDb(db, "postgres")}
}

func (st *Storage) Store(sp tracer.RawSpan) (err error) {
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

	_, err = tx.Exec(`INSERT INTO spans (id, trace_id, start_time, end_time, operation_name) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO UPDATE SET start_time = $3, end_time = $4, operation_name = $5`,
		int64(sp.SpanID), int64(sp.TraceID), sp.StartTime, sp.FinishTime, sp.OperationName)
	if err != nil {
		return err
	}

	if sp.ParentID != 0 {
		_, err = tx.Exec(`INSERT INTO spans (id, trace_id, start_time, end_time, operation_name) VALUES ($1, $2, $3, $4, '') ON CONFLICT (id) DO NOTHING`,
			int64(sp.ParentID), int64(sp.TraceID), time.Time{}, time.Time{})
		if err != nil {
			return err
		}

		_, err = tx.Exec(`INSERT INTO relations (span1_id, span2_id, kind) VALUES ($1, $2, 'parent')`,
			int64(sp.ParentID), int64(sp.SpanID))
		if err != nil {
			return err
		}
	}

	for k, v := range sp.Tags {
		vs := fmt.Sprintf("%v", v) // XXX
		_, err = tx.Exec(`INSERT INTO tags (span_id, key, value) VALUES ($1, $2, $3)`,
			int64(sp.SpanID), k, vs)
		if err != nil {
			return err
		}
	}
	for _, l := range sp.Logs {
		v := fmt.Sprintf("%v", l.Payload) // XXX
		_, err = tx.Exec(`INSERT INTO tags (span_id, key, value, time) VALUES ($1, $2, $3, $4)`,
			int64(sp.SpanID), l.Event, v, l.Timestamp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *Storage) TraceWithID(id uint64) (tracer.RawTrace, error) {
	tx, err := st.db.Begin()
	if err != nil {
		return tracer.RawTrace{}, err
	}
	defer tx.Rollback()
	return st.traceWithID(tx, id)
}

func (st *Storage) traceWithID(tx *sql.Tx, id uint64) (tracer.RawTrace, error) {
	rows, err := tx.Query(`SELECT spans.id, spans.trace_id, spans.start_time, spans.end_time, spans.operation_name, tags.key, tags.value, tags.time FROM spans LEFT JOIN tags ON spans.id = tags.span_id WHERE spans.trace_id = $1 ORDER BY spans.start_time ASC, spans.id`,
		int64(id))
	if err != nil {
		return tracer.RawTrace{}, err
	}

	spans, err := scanSpans(rows)
	if err != nil {
		return tracer.RawTrace{}, err
	}
	return tracer.RawTrace{
		TraceID: id,
		Spans:   spans,
	}, nil
}

func scanSpans(rows *sql.Rows) ([]tracer.RawSpan, error) {
	// TODO select parents
	var spans []tracer.RawSpan
	var (
		prevSpanID int64

		spanID        int64
		traceID       int64
		startTime     time.Time
		endTime       time.Time
		operationName string
		tagKey        string
		tagValue      string
		tagTime       *time.Time
	)
	tagTime = new(time.Time)
	var span tracer.RawSpan
	for rows.Next() {
		if err := rows.Scan(&spanID, &traceID, &startTime, &endTime, &operationName, &tagKey, &tagValue, &tagTime); err != nil {
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
		span.StartTime = startTime
		span.FinishTime = endTime
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

func (st *Storage) SpanWithID(id uint64) (tracer.RawSpan, error) {
	tx, err := st.db.Begin()
	if err != nil {
		return tracer.RawSpan{}, err
	}
	defer tx.Rollback()
	return st.spanWithID(tx, id)
}

func (st *Storage) spanWithID(tx *sql.Tx, id uint64) (tracer.RawSpan, error) {
	rows, err := tx.Query(`SELECT spans.id, spans.trace_id, spans.start_time, spans.end_time, spans.operation_name, tags.key, tags.value, tags.time FROM spans LEFT JOIN tags ON spans.id = tags.span_id WHERE id = $1 LIMIT 1`,
		int64(id))
	if err != nil {
		return tracer.RawSpan{}, err
	}
	spans, err := scanSpans(rows)
	if err != nil {
		return tracer.RawSpan{}, err
	}
	if len(spans) == 0 {
		return tracer.RawSpan{}, sql.ErrNoRows
	}
	return spans[0], nil
}

func (st *Storage) QueryTraces(q tracer.Query) ([]tracer.RawTrace, error) {
	tx, err := st.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var andConds []string
	var andArgs []interface{}
	var orConds []string
	var orArgs []interface{}
	if !q.StartTime.IsZero() {
		andConds = append(andConds, `(start_time >= ?)`)
		andArgs = append(andArgs, q.StartTime)
	}
	if !q.FinishTime.IsZero() {
		andConds = append(andConds, `(end_time <= ?)`)
		andArgs = append(andArgs, q.FinishTime)
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
	query := st.db.Rebind("SELECT DISTINCT spans.trace_id, spans.start_time FROM spans LEFT JOIN tags ON spans.id = tags.span_id WHERE " + strings.Join(conds, " AND ") + " ORDER BY spans.start_time ASC, spans.trace_id")
	fmt.Println(query)
	args := make([]interface{}, 0, len(andArgs)+len(orArgs))
	args = append(args, andArgs...)
	args = append(args, orArgs...)

	var ids []int64
	rows, err := st.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	var id int64
	var tmp time.Time
	for rows.Next() {
		if err := rows.Scan(&id, &tmp); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var traces []tracer.RawTrace
	for _, id := range ids {
		trace, err := st.traceWithID(tx, uint64(id))
		if err != nil {
			return nil, err
		}
		traces = append(traces, trace)
	}
	return traces, nil
}
