package bolt

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"honnef.co/go/tracer"

	"github.com/boltdb/bolt"
	"github.com/opentracing/opentracing-go"
)

var _ tracer.Storer = (*Storage)(nil)
var _ tracer.IDGenerator = (*Storage)(nil)

type Storage struct {
	db *bolt.DB
}

func New(path string) (*Storage, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("ids")); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("spans")); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("indexes")); err != nil {
			return err
		}
		return nil
	})
	return &Storage{db: db}, nil
}

func (st *Storage) GenerateID() uint64 {
	var id uint64
	st.db.Update(func(tx *bolt.Tx) error {
		ids := tx.Bucket([]byte("ids"))
		id, _ = ids.NextSequence()
		return nil
	})
	return id
}

func buildID(sp *tracer.Span) string {
	return fmt.Sprintf("%016x-%016x-%016x", sp.TraceID, sp.ParentID, sp.SpanID)
}

func (st *Storage) AddSpan(sp *tracer.Span) {
	id := buildID(sp)
	st.db.Update(func(tx *bolt.Tx) error {
		spans := tx.Bucket([]byte("spans"))
		if err := spans.Put([]byte(id), nil); err != nil {
			return err
		}
		if err := spans.Put([]byte(id+"/operation_name"), []byte(sp.OperationName)); err != nil {
			return err
		}
		if err := spans.Put([]byte(id+"/start_time"), []byte(sp.StartTime.Format(time.RFC3339))); err != nil {
			return err
		}

		indexes := tx.Bucket([]byte("indexes"))
		return indexes.Put([]byte(fmt.Sprintf("%016x", sp.SpanID)), []byte(id))
	})
}

func (st *Storage) SetOperationName(sp *tracer.Span, name string) {
	id := buildID(sp)
	st.db.Update(func(tx *bolt.Tx) error {
		spans := tx.Bucket([]byte("spans"))
		return spans.Put([]byte(id+"/operation_name"), []byte(name))
	})
}

func (st *Storage) SetTag(sp *tracer.Span, key string, value interface{}) {
	id := buildID(sp)
	st.db.Update(func(tx *bolt.Tx) error {
		spans := tx.Bucket([]byte("spans"))
		val, _ := json.Marshal(value)
		return spans.Put([]byte(id+"/tags/"+key), []byte(val))
	})
}

func (st *Storage) Log(sp *tracer.Span, data opentracing.LogData) {
	st.db.Update(func(tx *bolt.Tx) error {
		return st.log(sp, data, tx)
	})
}

func (st *Storage) log(sp *tracer.Span, data opentracing.LogData, tx *bolt.Tx) error {
	id := buildID(sp)
	spans := tx.Bucket([]byte("spans"))
	next, _ := spans.NextSequence()
	nexts := strconv.FormatUint(next, 10)
	key := id + "/logs/" + nexts + "/"
	if err := spans.Put([]byte(key+"event"), []byte(data.Event)); err != nil {
		return err
	}
	if data.Payload != nil {
		payload, _ := json.Marshal(data.Payload)
		if err := spans.Put([]byte(key+"payload"), []byte(payload)); err != nil {
			return err
		}
	}
	if err := spans.Put([]byte(key+"timestamp"), []byte(data.Timestamp.Format(time.RFC3339))); err != nil {
		return err
	}
	return nil
}

func (st *Storage) FinishWithOptions(sp *tracer.Span, opts opentracing.FinishOptions) {
	id := buildID(sp)
	st.db.Update(func(tx *bolt.Tx) error {
		spans := tx.Bucket([]byte("spans"))
		if err := spans.Put([]byte(id+"/finish_time"), []byte(opts.FinishTime.Format(time.RFC3339))); err != nil {
			return err
		}
		for _, log := range opts.BulkLogData {
			if err := st.log(sp, log, tx); err != nil {
				return err
			}
		}
		return nil
	})
}
