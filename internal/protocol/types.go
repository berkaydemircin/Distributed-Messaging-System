package protocol

import (
	"time"
)

type Message struct {
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

type Batch struct {
	FirstOffset     uint64
	Attributes      uint16 // use for compression, and potentially other things like timestamps, for now just a stub
	FirstTimestamp  time.Time
	MaxTimestamp    time.Time
	LastOffsetDelta uint32 // will use for retention
	Messages        []*Message
}
