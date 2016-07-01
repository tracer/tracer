package bolt

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"honnef.co/go/tracer"

	"github.com/boltdb/bolt"
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

func (st *Storage) Store(sp *tracer.Span) {
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
		if err := spans.Put([]byte(id+"/finish_time"), []byte(sp.FinishTime.Format(time.RFC3339))); err != nil {
			return err
		}
		for k, v := range sp.Tags {
			val, _ := json.Marshal(v)
			if err := spans.Put([]byte(id+"/tags/"+k), []byte(val)); err != nil {
				return err
			}
		}
		for _, log := range sp.Logs {
			next, _ := spans.NextSequence()
			nexts := strconv.FormatUint(next, 10)
			key := id + "/logs/" + nexts + "/"
			if err := spans.Put([]byte(key+"event"), []byte(log.Event)); err != nil {
				return err
			}
			if log.Payload != nil {
				payload, _ := json.Marshal(log.Payload)
				if err := spans.Put([]byte(key+"payload"), []byte(payload)); err != nil {
					return err
				}
			}
			if err := spans.Put([]byte(key+"timestamp"), []byte(log.Timestamp.Format(time.RFC3339Nano))); err != nil {
				return err
			}
		}

		indexes := tx.Bucket([]byte("indexes"))
		return indexes.Put([]byte(fmt.Sprintf("%016x", sp.SpanID)), []byte(id))
	})
}
