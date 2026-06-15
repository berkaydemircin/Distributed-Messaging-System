package broker

import (
	"fmt"

	storagelog "github.com/berkaydemircin/Distributed-Messaging-System/internal/log"
	"github.com/berkaydemircin/Distributed-Messaging-System/internal/protocol"
	"github.com/berkaydemircin/Distributed-Messaging-System/internal/server"
)

type fetchStreamResponse struct {
	throttleTimeMs int32
	errorCode      int16
	sessionID      int32
	topics         []fetchStreamTopic
}

type fetchStreamTopic struct {
	name       string
	partitions []fetchStreamPartition
}

type fetchStreamPartition struct {
	index                int32
	errorCode            int16
	highWatermark        int64
	lastStableOffset     int64
	logStartOffset       int64
	preferredReadReplica int32

	recordsSize int64
	ranges      []storagelog.RawRange
}

func encodeFetchStreamResponse(resp *fetchStreamResponse, apiVersion int16) server.Response {
	var parts []server.ResponsePart
	e := protocol.NewEncoder(512)

	flush := func() {
		if e.Len() == 0 {
			return
		}
		// note that we immediately mutate e so this should be safe
		parts = append(parts, server.BytesPart(e.Bytes()))
		e = protocol.NewEncoder(256)
	}

	if apiVersion >= 1 {
		e.PutInt32(resp.throttleTimeMs)
	}
	if apiVersion >= 7 {
		e.PutInt16(resp.errorCode)
		e.PutInt32(resp.sessionID)
	}

	e.PutArrayLen(int32(len(resp.topics)))
	for _, t := range resp.topics {
		e.PutString(t.name)
		e.PutArrayLen(int32(len(t.partitions)))

		for _, p := range t.partitions {
			e.PutInt32(p.index)
			e.PutInt16(p.errorCode)
			e.PutInt64(p.highWatermark)

			if apiVersion >= 4 {
				e.PutInt64(p.lastStableOffset)
			}
			if apiVersion >= 5 {
				e.PutInt64(p.logStartOffset)
			}
			if apiVersion >= 4 {
				e.PutArrayLen(-1) // aborted_transactions=null
			}
			if apiVersion >= 11 {
				e.PutInt32(p.preferredReadReplica)
			}

			if p.recordsSize < 0 || p.recordsSize > int64(1<<31-1) {
				panic(fmt.Sprintf("fetch records size out of int32 range: %d", p.recordsSize))
			}

			// non-null empty records field as length=0, not null length=-1
			e.PutInt32(int32(p.recordsSize))
			flush()

			for _, r := range p.ranges {
				parts = append(parts, server.FilePart{
					File:   r.File,
					Offset: r.Offset,
					Length: r.Length,
				})
			}
		}
	}

	flush()
	return server.NewCompositeResponse(parts)
}
