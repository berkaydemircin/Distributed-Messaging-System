package protocol

import (
	"time"
)

// may remove per message ts later, needs to be read up on

type Message struct {
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

type Batch struct {
	FirstOffset     uint64
	Attributes      uint16 // use for compression, and potentially other things like timestamps, for now just a stub
	FirstTimestamp  time.Time
	MaxTimestamp    time.Time // will use for retention or first ts? -> max makes more sense
	LastOffsetDelta uint32
	Messages        []*Message
}
