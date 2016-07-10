package pbutil

import (
	"time"

	"github.com/golang/protobuf/ptypes"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
)

// Timestamp converts a protobuf timestamp to a Go time.Time. It
// returns the zero time when ts is nil.
func Timestamp(ts *tspb.Timestamp) (time.Time, error) {
	if ts == nil {
		return time.Time{}, nil
	}
	return ptypes.Timestamp(ts)
}
