package protocol

type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      *string
}

func ParseRequestHeader(msg []byte) (RequestHeader, []byte, error) {
	d := NewDecoder(msg)

	var h RequestHeader
	h.APIKey = d.Int16() // first 4 bytes
	h.APIVersion = d.Int16()
	h.CorrelationID = d.Int32()

	headerVersion := RequestHeaderVersion(h.APIKey, h.APIVersion)

	if headerVersion >= 1 {
		h.ClientID = d.NullableString()
	}
	if headerVersion >= 2 {
		d.DiscardTaggedFields()
	}

	if d.Error() != nil {
		return RequestHeader{}, nil, d.Error()
	}
	return h, msg[d.Offset():], nil
}

// v0: correlation_id only - v1 (flexible): + empty tagged fields
func WriteResponseHeader(enc *Encoder, correlationID int32, apiKey, apiVersion int16) {
	enc.PutInt32(correlationID)
	if ResponseHeaderVersion(apiKey, apiVersion) >= 1 {
		enc.PutTaggedFields()
	}
}
