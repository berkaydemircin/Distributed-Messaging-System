//go:build !linux

package server

import (
	"io"
	"net"
	"os"
)

func sendFileRange(dst *net.TCPConn, src *os.File, offset int64, length int64) error {
	_, err := io.CopyN(dst, io.NewSectionReader(src, offset, length), length)
	return err
}
