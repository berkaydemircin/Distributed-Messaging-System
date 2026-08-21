package protocol

/* Fetch API v4-v11, non-flexible encoding.
	Version-specific fields:
	  Request:
	    v3+:  max_bytes
	    v4+:  isolation_level
	    v7+:  session_id, session_epoch, forgotten_topics_data
	    v9+:  current_leader_epoch per partition
	    v11+: rack_id
	  Response:
	    v1+:  throttle_time_ms
	    v7+:  top-level error_code, session_id
	    v4+:  last_stable_offset, aborted_transactions per partition
	    v5+:  log_start_offset per partition
	    v11+: preferred_read_replica per partition
    Fetch sessions are not implemented, we always return session_id=0.
*/

// ── Request ───────────────────────────────────────────────────────────────────

type FetchRequest struct {
	ReplicaID       int32
	MaxWaitMs       int32
	MinBytes        int32
	MaxBytes        int32 // v3+, default 0x7fffffff
	IsolationLevel  int8  // v4+, 0=READ_UNCOMMITTED
	SessionID       int32 // v7+, 0 = no session
	SessionEpoch    int32 // v7+, -1 = no session
	Topics          []FetchRequestTopic
	ForgottenTopics []FetchForgottenTopic // v7+
	RackID          string                // v11+
}

type FetchRequestTopic struct {
	Name       string
	Partitions []FetchRequestPartition
}

type FetchRequestPartition struct {
	Index              int32
	CurrentLeaderEpoch int32 // v9+, -1 = ignore
	FetchOffset        int64
	LogStartOffset     int64 // v5+, follower-only, consumers send -1
	MaxBytes           int32
}

type FetchForgottenTopic struct {
	Name       string
	Partitions []int32
}

func EncodeFetchRequest(req *FetchRequest, apiVersion int16) []byte {
	e := NewEncoder(256)

	e.PutInt32(req.ReplicaID)
	e.PutInt32(req.MaxWaitMs)
	e.PutInt32(req.MinBytes)

	if apiVersion >= 3 {
		e.PutInt32(req.MaxBytes)
	}
	if apiVersion >= 4 {
		e.PutInt8(req.IsolationLevel)
	}
	if apiVersion >= 7 {
		e.PutInt32(req.SessionID)
		e.PutInt32(req.SessionEpoch)
	}

	e.PutArrayLen(int32(len(req.Topics)))
	for _, t := range req.Topics {
		e.PutString(t.Name)

		e.PutArrayLen(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			e.PutInt32(p.Index)
			if apiVersion >= 9 {
				e.PutInt32(p.CurrentLeaderEpoch)
			}
			e.PutInt64(p.FetchOffset)
			if apiVersion >= 5 {
				e.PutInt64(p.LogStartOffset)
			}
			e.PutInt32(p.MaxBytes)
		}
	}

	if apiVersion >= 7 {
		e.PutArrayLen(int32(len(req.ForgottenTopics)))
		for _, ft := range req.ForgottenTopics {
			e.PutString(ft.Name)
			e.PutArrayLen(int32(len(ft.Partitions)))
			for _, idx := range ft.Partitions {
				e.PutInt32(idx)
			}
		}
	}

	if apiVersion >= 11 {
		e.PutString(req.RackID)
	}

	return e.Bytes()
}

func DecodeFetchRequest(body []byte, apiVersion int16) (*FetchRequest, error) {
	d := NewDecoder(body)
	req := &FetchRequest{
		MaxBytes:     0x7fffffff, // default per spec
		SessionID:    0,
		SessionEpoch: -1,
	}

	req.ReplicaID = d.Int32()
	req.MaxWaitMs = d.Int32()
	req.MinBytes = d.Int32()

	if apiVersion >= 3 {
		req.MaxBytes = d.Int32()
	}

	if apiVersion >= 4 {
		req.IsolationLevel = d.Int8()
	}

	if apiVersion >= 7 {
		req.SessionID = d.Int32()
		req.SessionEpoch = d.Int32()
	}

	numTopics := max(d.ArrayLen(), 0)

	req.Topics = make([]FetchRequestTopic, numTopics)
	for i := range req.Topics {
		req.Topics[i].Name = d.String()

		numParts := max(d.ArrayLen(), 0)

		req.Topics[i].Partitions = make([]FetchRequestPartition, numParts)
		for j := range req.Topics[i].Partitions {
			p := &req.Topics[i].Partitions[j]

			p.Index = d.Int32()

			if apiVersion >= 9 {
				p.CurrentLeaderEpoch = d.Int32()
			} else {
				p.CurrentLeaderEpoch = -1
			}

			p.FetchOffset = d.Int64()

			if apiVersion >= 5 {
				p.LogStartOffset = d.Int64()
			} else {
				p.LogStartOffset = -1
			}

			p.MaxBytes = d.Int32()
		}
	}

	if apiVersion >= 7 {
		numForgotten := max(d.ArrayLen(), 0)

		req.ForgottenTopics = make([]FetchForgottenTopic, numForgotten)
		for i := range req.ForgottenTopics {
			req.ForgottenTopics[i].Name = d.String()

			numParts := d.ArrayLen()
			if numParts < 0 {
				numParts = 0
			}

			req.ForgottenTopics[i].Partitions = make([]int32, numParts)
			for j := range req.ForgottenTopics[i].Partitions {
				req.ForgottenTopics[i].Partitions[j] = d.Int32()
			}
		}
	}

	if apiVersion >= 11 {
		req.RackID = d.String()
	}

	if d.Error() != nil {
		return nil, d.Error()
	}

	return req, nil
}

type FetchResponse struct {
	ThrottleTimeMs int32 // v1+
	ErrorCode      int16 // v7+, always 0
	SessionID      int32 // v7+, always 0 = stateless
	Topics         []FetchResponseTopic
}

type FetchResponseTopic struct {
	Name       string
	Partitions []FetchResponsePartition
}

type FetchResponsePartition struct {
	Index                int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64 // v4+: = HighWatermark for non transactional
	LogStartOffset       int64 // v5+
	PreferredReadReplica int32 // v11+: -1 = use leader

	Records []byte
}

func DecodeFetchResponse(body []byte, apiVersion int16) (*FetchResponse, error) {
	d := NewDecoder(body)
	resp := &FetchResponse{}

	if apiVersion >= 1 {
		resp.ThrottleTimeMs = d.Int32()
	}
	if apiVersion >= 7 {
		resp.ErrorCode = d.Int16()
		resp.SessionID = d.Int32()
	}

	numTopics := max(d.ArrayLen(), 0)
	resp.Topics = make([]FetchResponseTopic, numTopics)
	for i := range resp.Topics {
		resp.Topics[i].Name = d.String()

		numParts := max(d.ArrayLen(), 0)
		resp.Topics[i].Partitions = make([]FetchResponsePartition, numParts)
		for j := range resp.Topics[i].Partitions {
			p := &resp.Topics[i].Partitions[j]

			p.Index = d.Int32()
			p.ErrorCode = d.Int16()
			p.HighWatermark = d.Int64()

			if apiVersion >= 4 {
				p.LastStableOffset = d.Int64()
			}
			if apiVersion >= 5 {
				p.LogStartOffset = d.Int64()
			}
			if apiVersion >= 4 {
				abortedTransactions := d.ArrayLen()
				for k := int32(0); k < abortedTransactions; k++ {
					d.Int64() // producer_id
					d.Int64() // first_offset
				}
			}
			if apiVersion >= 11 {
				p.PreferredReadReplica = d.Int32()
			}

			p.Records = d.NullableBytes()
		}
	}

	if d.Error() != nil {
		return nil, d.Error()
	}
	return resp, nil
}

func EncodeFetchResponse(resp *FetchResponse, apiVersion int16) []byte {
	e := NewEncoder(512)

	if apiVersion >= 1 {
		e.PutInt32(resp.ThrottleTimeMs)
	}

	if apiVersion >= 7 {
		e.PutInt16(resp.ErrorCode)
		e.PutInt32(resp.SessionID)
	}

	e.PutArrayLen(int32(len(resp.Topics)))
	for _, t := range resp.Topics {
		e.PutString(t.Name)

		e.PutArrayLen(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			e.PutInt32(p.Index)
			e.PutInt16(p.ErrorCode)
			e.PutInt64(p.HighWatermark)

			if apiVersion >= 4 {
				e.PutInt64(p.LastStableOffset)
			}

			if apiVersion >= 5 {
				e.PutInt64(p.LogStartOffset)
			}

			if apiVersion >= 4 {
				e.PutArrayLen(-1)
			}

			if apiVersion >= 11 {
				e.PutInt32(p.PreferredReadReplica)
			}

			putNonNullRecords(e, p.Records)
		}
	}

	return e.Bytes()
}

func putNonNullRecords(e *Encoder, records []byte) {
	if records == nil {
		records = []byte{}
	}

	e.PutNullableBytes(records)
}
