package protocol

/*
 * ApiVersions (API key 18) — the bootstrap API.
 * Supported versions: v0 (non-flexible) and v3 (flexible).
 *
 * v0 request: empty body.
 * v3 request: client_software_name COMPACT_STRING,
 *             client_software_version COMPACT_STRING, tagged_fields.
 *
 * v0 response: error_code INT16, api_versions ARRAY[api_key, min, max].
 * v3 response: error_code INT16, api_versions COMPACT_ARRAY[api_key, min, max, tagged_fields],
 *              throttle_time_ms INT32, tagged_fields.
 *
 * Response header is ALWAYS v0 (correlation_id only), even for v3.
 */

// EncodeApiVersionsResponseV0 encodes a v0 response body.
func EncodeApiVersionsResponseV0(errorCode int16) []byte {
	e := NewEncoder(6 + len(SupportedAPIVersions)*6)
	e.PutInt16(errorCode)
	e.PutArrayLen(int32(len(SupportedAPIVersions)))
	for _, v := range SupportedAPIVersions {
		e.PutInt16(v.Key)
		e.PutInt16(v.MinVersion)
		e.PutInt16(v.MaxVersion)
	}
	return e.Bytes()
}

// EncodeApiVersionsResponseV3 encodes a v3 (flexible) response body.
func EncodeApiVersionsResponseV3(errorCode int16) []byte {
	e := NewEncoder(16 + len(SupportedAPIVersions)*8)
	e.PutInt16(errorCode)

	// COMPACT_ARRAY of ApiVersion entries
	e.PutCompactArrayLen(len(SupportedAPIVersions))
	for _, v := range SupportedAPIVersions {
		e.PutInt16(v.Key)
		e.PutInt16(v.MinVersion)
		e.PutInt16(v.MaxVersion)
		e.PutTaggedFields() // per-entry tagged fields
	}

	e.PutInt32(0)       // throttle_time_ms
	e.PutTaggedFields() // top-level tagged fields
	return e.Bytes()
}
