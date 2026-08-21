package protocol

// OffsetForLeaderEpoch supports legacy v2-v3 and flexible v4 encoding

type OffsetsForLeaderEpochRequest struct {
	ReplicaID int32 // v3+, -2 when absent
	Topics    []OffsetsForLeaderEpochRequestTopic
}

type OffsetsForLeaderEpochRequestTopic struct {
	Name       string
	Partitions []OffsetsForLeaderEpochRequestPartition
}

type OffsetsForLeaderEpochRequestPartition struct {
	Index              int32
	CurrentLeaderEpoch int32
	LeaderEpoch        int32
}

func decodeOFLEArrayLen(d *Decoder, flexible bool) int32 {
	if flexible {
		return int32(max(d.CompactArrayLen(), 0))
	}
	return max(d.ArrayLen(), 0)
}

func decodeOFLEString(d *Decoder, flexible bool) string {
	if flexible {
		return d.CompactString()
	}
	return d.String()
}

func encodeOFLEArrayLen(e *Encoder, n int, flexible bool) {
	if flexible {
		e.PutCompactArrayLen(n)
	} else {
		e.PutArrayLen(int32(n))
	}
}

func encodeOFLEString(e *Encoder, s string, flexible bool) {
	if flexible {
		e.PutCompactString(s)
	} else {
		e.PutString(s)
	}
}

func EncodeOffsetsForLeaderEpochRequest(req *OffsetsForLeaderEpochRequest, apiVersion int16) []byte {
	flexible := apiVersion >= 4
	e := NewEncoder(128)

	if apiVersion >= 3 {
		e.PutInt32(req.ReplicaID)
	}

	encodeOFLEArrayLen(e, len(req.Topics), flexible)
	for _, t := range req.Topics {
		encodeOFLEString(e, t.Name, flexible)

		encodeOFLEArrayLen(e, len(t.Partitions), flexible)
		for _, p := range t.Partitions {
			e.PutInt32(p.Index)
			e.PutInt32(p.CurrentLeaderEpoch)
			e.PutInt32(p.LeaderEpoch)
			if flexible {
				e.PutTaggedFields()
			}
		}
		if flexible {
			e.PutTaggedFields()
		}
	}
	if flexible {
		e.PutTaggedFields()
	}

	return e.Bytes()
}

func DecodeOffsetsForLeaderEpochRequest(body []byte, apiVersion int16) (*OffsetsForLeaderEpochRequest, error) {
	d := NewDecoder(body)
	flexible := apiVersion >= 4
	req := &OffsetsForLeaderEpochRequest{ReplicaID: -2}

	if apiVersion >= 3 {
		req.ReplicaID = d.Int32()
	}

	numTopics := decodeOFLEArrayLen(d, flexible)
	req.Topics = make([]OffsetsForLeaderEpochRequestTopic, numTopics)
	for i := range req.Topics {
		req.Topics[i].Name = decodeOFLEString(d, flexible)

		numParts := decodeOFLEArrayLen(d, flexible)
		req.Topics[i].Partitions = make([]OffsetsForLeaderEpochRequestPartition, numParts)
		for j := range req.Topics[i].Partitions {
			p := &req.Topics[i].Partitions[j]
			p.Index = d.Int32()
			p.CurrentLeaderEpoch = d.Int32()
			p.LeaderEpoch = d.Int32()
			if flexible {
				d.DiscardTaggedFields()
			}
		}
		if flexible {
			d.DiscardTaggedFields()
		}
	}
	if flexible {
		d.DiscardTaggedFields()
	}

	if d.Error() != nil {
		return nil, d.Error()
	}
	return req, nil
}

type OffsetsForLeaderEpochResponse struct {
	ThrottleTimeMs int32
	Topics         []OffsetsForLeaderEpochResponseTopic
}

type OffsetsForLeaderEpochResponseTopic struct {
	Name       string
	Partitions []OffsetsForLeaderEpochResponsePartition
}

type OffsetsForLeaderEpochResponsePartition struct {
	ErrorCode   int16
	Index       int32
	LeaderEpoch int32
	EndOffset   int64
}

func EncodeOffsetsForLeaderEpochResponse(resp *OffsetsForLeaderEpochResponse, apiVersion int16) []byte {
	flexible := apiVersion >= 4
	e := NewEncoder(128)

	e.PutInt32(resp.ThrottleTimeMs)

	if flexible {
		e.PutCompactArrayLen(len(resp.Topics))
	} else {
		e.PutArrayLen(int32(len(resp.Topics)))
	}
	for _, t := range resp.Topics {
		if flexible {
			e.PutCompactString(t.Name)
		} else {
			e.PutString(t.Name)
		}

		if flexible {
			e.PutCompactArrayLen(len(t.Partitions))
		} else {
			e.PutArrayLen(int32(len(t.Partitions)))
		}
		for _, p := range t.Partitions {
			e.PutInt16(p.ErrorCode)
			e.PutInt32(p.Index)
			e.PutInt32(p.LeaderEpoch)
			e.PutInt64(p.EndOffset)
			if flexible {
				e.PutTaggedFields()
			}
		}
		if flexible {
			e.PutTaggedFields()
		}
	}
	if flexible {
		e.PutTaggedFields()
	}

	return e.Bytes()
}

func DecodeOffsetsForLeaderEpochResponse(body []byte, apiVersion int16) (*OffsetsForLeaderEpochResponse, error) {
	flexible := apiVersion >= 4
	d := NewDecoder(body)
	resp := &OffsetsForLeaderEpochResponse{}

	resp.ThrottleTimeMs = d.Int32()

	numTopics := decodeOFLEArrayLen(d, flexible)
	resp.Topics = make([]OffsetsForLeaderEpochResponseTopic, numTopics)
	for i := range resp.Topics {
		resp.Topics[i].Name = decodeOFLEString(d, flexible)

		numParts := decodeOFLEArrayLen(d, flexible)
		resp.Topics[i].Partitions = make([]OffsetsForLeaderEpochResponsePartition, numParts)
		for j := range resp.Topics[i].Partitions {
			p := &resp.Topics[i].Partitions[j]
			p.ErrorCode = d.Int16()
			p.Index = d.Int32()
			p.LeaderEpoch = d.Int32()
			p.EndOffset = d.Int64()
			if flexible {
				d.DiscardTaggedFields()
			}
		}
		if flexible {
			d.DiscardTaggedFields()
		}
	}
	if flexible {
		d.DiscardTaggedFields()
	}

	if d.Error() != nil {
		return nil, d.Error()
	}
	return resp, nil
}
