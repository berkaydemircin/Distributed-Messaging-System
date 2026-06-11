package protocol

const (
	APIKeyProduce     int16 = 0
	APIKeyFetch       int16 = 1
	APIKeyListOffsets int16 = 2
	APIKeyMetadata    int16 = 3
	APIKeyApiVersions int16 = 18
)

const (
	ErrCodeUnsupportedVersion int16 = 35
)

type APIVersion struct {
	Key        int16
	MinVersion int16
	MaxVersion int16
}

// set of apis the broker advertises
// WARNING handler returns UNSUPPORTED_VERSION for v0 - v2
var SupportedAPIVersions = []APIVersion{
	{APIKeyProduce, 0, 8},
	{APIKeyFetch, 4, 11},
	{APIKeyListOffsets, 1, 5},
	{APIKeyMetadata, 0, 8},
	{APIKeyApiVersions, 0, 3},
}

// Returns the request header version for a given API key and API version. v1 = standard, v2 = flexible (tagged fields)
func RequestHeaderVersion(apiKey, apiVersion int16) int16 {
	switch apiKey {
	case APIKeyApiVersions:
		if apiVersion >= 3 {
			return 2
		}
	case APIKeyProduce:
		if apiVersion >= 9 {
			return 2
		}
	case APIKeyFetch:
		if apiVersion >= 12 {
			return 2
		}
	case APIKeyMetadata:
		if apiVersion >= 9 {
			return 2
		}
	}
	return 1
}

func ResponseHeaderVersion(apiKey, apiVersion int16) int16 {
	if apiKey == APIKeyApiVersions {
		return 0
	}
	switch apiKey {
	case APIKeyProduce:
		if apiVersion >= 9 {
			return 1
		}
	case APIKeyFetch:
		if apiVersion >= 12 {
			return 1
		}
	case APIKeyMetadata:
		if apiVersion >= 9 {
			return 1
		}
	}
	return 0
}
