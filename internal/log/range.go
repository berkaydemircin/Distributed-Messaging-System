package log

import "os"

type RawRange struct {
	File   *os.File
	Offset int64
	Length int64
}

type ReadRawRangesResult struct {
	Ranges      []RawRange
	Bytes       int64
	FetchedUpTo uint64
}
