package protocol

type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      *string
}

func EncodeRequestHeader(e *Encoder, apiKey, apiVersion int16, correlationID int32, clientID *string) {
	e.PutInt16(apiKey)
	e.PutInt16(apiVersion)
	e.PutInt32(correlationID)

	headerVersion := RequestHeaderVersion(apiKey, apiVersion)
	if headerVersion >= 1 {
		e.PutNullableString(clientID)
	}
	if headerVersion >= 2 {
		e.PutTaggedFields()
	}
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

func ParseResponseHeader(msg []byte, apiKey, apiVersion int16) (int32, []byte, error) {
	d := NewDecoder(msg)
	correlationID := d.Int32()
	if ResponseHeaderVersion(apiKey, apiVersion) >= 1 {
		d.DiscardTaggedFields()
	}
	if d.Error() != nil {
		return 0, nil, d.Error()
	}
	return correlationID, msg[d.Offset():], nil
}

// v0: correlation_id only - v1 (flexible): + empty tagged fields
func WriteResponseHeader(enc *Encoder, correlationID int32, apiKey, apiVersion int16) {
	enc.PutInt32(correlationID)
	if ResponseHeaderVersion(apiKey, apiVersion) >= 1 {
		enc.PutTaggedFields()
	}
}
