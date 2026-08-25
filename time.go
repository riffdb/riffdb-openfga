package riffdbopenfga

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	riffdb "riffdb.dev/application"
)

func protoTimestamp(value riffdb.Instant) *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(value.Seconds, int64(value.Nanos)).UTC())
}
