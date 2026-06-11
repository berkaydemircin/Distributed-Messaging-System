package protocol

/*
 * Produce (API key 0) - v3–v8 (legacy encoding, non flexible).
 *
 * Version matrix for fields we care about:
 *   Request  - no new fields between v3 and v8 (transactional_id added in v3)
 *   Response v2+: log_append_time_ms
 *            v5+: log_start_offset
 *            v8+: record_errors ARRAY, error_message NULLABLE_STRING
 *            v1+: throttle_time_ms
 */

type ProduceRequest struct {
	TransactionalID *string
	Acks            int16
	TimeoutMs       int32
	Topics          []ProduceRequestTopic
}

type ProduceRequestTopic struct {
	Name       string
	Partitions []ProduceRequestPartition
}

type ProduceRequestPartition struct {
	Index   int32
	Records []byte
}

func DecodeProduceRequest(body []byte, apiVersion int16) (*ProduceRequest, error) {
	d := NewDecoder(body)
	req := &ProduceRequest{}

	// transactional_id present from v3 onward (we only handle v3+)
	req.TransactionalID = d.NullableString()
	req.Acks = d.Int16()
	req.TimeoutMs = d.Int32()

	numTopics := max(d.ArrayLen(), 0)
	req.Topics = make([]ProduceRequestTopic, numTopics)
	for i := range req.Topics {
		req.Topics[i].Name = d.String()
		numParts := max(d.ArrayLen(), 0)
		req.Topics[i].Partitions = make([]ProduceRequestPartition, numParts)
		for j := range req.Topics[i].Partitions {
			req.Topics[i].Partitions[j].Index = d.Int32()
			req.Topics[i].Partitions[j].Records = d.NullableBytes()
		}
	}

	if d.Error() != nil {
		return nil, d.Error()
	}
	_ = apiVersion // reserved for future use, request body is uniform across v3-v8
	return req, nil
}

type ProduceResponse struct {
	Topics       []ProduceResponseTopic
	ThrottleTime int32
}

type ProduceResponseTopic struct {
	Name       string
	Partitions []ProduceResponsePartition
}

type ProduceResponsePartition struct {
	Index          int32
	ErrorCode      int16
	BaseOffset     int64
	LogAppendTime  int64                // v2+: -1 for CreateTime mode
	LogStartOffset int64                // v5+
	RecordErrors   []ProduceRecordError // v8+: empty on success
	ErrorMessage   *string              // v8+: nil on success
}

// which record caused batch to fail
type ProduceRecordError struct {
	BatchIndex int32
	Message    *string
}

func EncodeProduceResponse(resp *ProduceResponse, apiVersion int16) []byte {
	e := NewEncoder(256)

	e.PutArrayLen(int32(len(resp.Topics)))
	for _, t := range resp.Topics {
		e.PutString(t.Name)
		e.PutArrayLen(int32(len(t.Partitions)))
		for _, p := range t.Partitions {
			e.PutInt32(p.Index)
			e.PutInt16(p.ErrorCode)
			e.PutInt64(p.BaseOffset)

			if apiVersion >= 2 {
				e.PutInt64(p.LogAppendTime) // -1 for CreateTime
			}
			if apiVersion >= 5 {
				e.PutInt64(p.LogStartOffset)
			}
			if apiVersion >= 8 {
				e.PutArrayLen(int32(len(p.RecordErrors)))
				for _, re := range p.RecordErrors {
					e.PutInt32(re.BatchIndex)
					e.PutNullableString(re.Message)
				}
				e.PutNullableString(p.ErrorMessage)
			}
		}
	}

	if apiVersion >= 1 {
		e.PutInt32(resp.ThrottleTime)
	}

	return e.Bytes()
}
