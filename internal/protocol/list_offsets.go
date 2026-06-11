package protocol

/*
 * ListOffsets (API key 2) - v1–v5 (legacy encoding, non-flexible).
 *
 * Used by consumers to resolve special timestamps to actual offsets:
 *   -1 = LATEST    log end offset
 *   -2 = EARLIEST  log start offset
 *   >= 0           find offset by timestamp (not implemented here)
 *
 * Request (v1+):
 *   replica_id      INT32
 *   isolation_level  INT8   (v2+, 0=READ_UNCOMMITTED)
 *   topics          ARRAY[
 *     name          STRING
 *     partitions    ARRAY[
 *       partition_index INT32
 *       timestamp       INT64  - -1=LATEST, -2=EARLIEST
 *     ]
 *   ]
 *
 * Response (v1+):
 *   throttle_time_ms INT32   (v2+)
 *   topics           ARRAY[
 *     name           STRING
 *     partitions     ARRAY[
 *       partition_index INT32
 *       error_code      INT16
 *       timestamp       INT64  - -1 for LATEST/EARLIEST
 *       offset          INT64
 *     ]
 *   ]
 */

type ListOffsetsRequest struct {
	ReplicaID      int32
	IsolationLevel int8 // v2+, 0 = READ_UNCOMMITTED
	Topics         []ListOffsetsRequestTopic
}

type ListOffsetsRequestTopic struct {
	Name       string
	Partitions []ListOffsetsRequestPartition
}

type ListOffsetsRequestPartition struct {
	Index              int32
	CurrentLeaderEpoch int32 // v4+, -1 = ignore
	Timestamp          int64 // -1 = latest, -2 = earliest
}

func DecodeListOffsetsRequest(body []byte, apiVersion int16) (*ListOffsetsRequest, error) {
	d := NewDecoder(body)
	req := &ListOffsetsRequest{}

	req.ReplicaID = d.Int32()
	if apiVersion >= 2 {
		req.IsolationLevel = d.Int8()
	}

	numTopics := max(d.ArrayLen(), 0)
	req.Topics = make([]ListOffsetsRequestTopic, numTopics)
	for i := range req.Topics {
		req.Topics[i].Name = d.String()
		numParts := d.ArrayLen()
		if numParts < 0 {
			numParts = 0
		}
		req.Topics[i].Partitions = make([]ListOffsetsRequestPartition, numParts)
		for j := range req.Topics[i].Partitions {
			p := &req.Topics[i].Partitions[j]

			p.Index = d.Int32()

			if apiVersion >= 4 {
				p.CurrentLeaderEpoch = d.Int32()
			} else {
				p.CurrentLeaderEpoch = -1
			}

			p.Timestamp = d.Int64()
		}
	}

	if d.Error() != nil {
		return nil, d.Error()
	}
	return req, nil
}

type ListOffsetsResponse struct {
	ThrottleTimeMs int32 // v2+
	Topics         []ListOffsetsResponseTopic
}

type ListOffsetsResponseTopic struct {
	Name       string
	Partitions []ListOffsetsResponsePartition
}

type ListOffsetsResponsePartition struct {
	Index       int32
	ErrorCode   int16
	Timestamp   int64
	Offset      int64
	LeaderEpoch int32 // v4+
}

func EncodeListOffsetsResponse(resp *ListOffsetsResponse, apiVersion int16) []byte {
	e := NewEncoder(128)

	if apiVersion >= 2 {
		e.PutInt32(resp.ThrottleTimeMs)
	}

	e.PutArrayLen(int32(len(resp.Topics)))
	for _, t := range resp.Topics {
		e.PutString(t.Name)
		e.PutArrayLen(int32(len(t.Partitions)))

		for _, p := range t.Partitions {
			e.PutInt32(p.Index)
			e.PutInt16(p.ErrorCode)
			e.PutInt64(p.Timestamp)
			e.PutInt64(p.Offset)

			if apiVersion >= 4 {
				e.PutInt32(p.LeaderEpoch)
			}
		}
	}

	return e.Bytes()
}
