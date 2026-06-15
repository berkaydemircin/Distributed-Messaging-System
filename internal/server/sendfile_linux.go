//go:build linux

package server

import (
	"errors"
	"io"
	"net"
	"os"
	"syscall"
)

func sendFileRange(dst *net.TCPConn, src *os.File, offset int64, length int64) error {
	rc, err := dst.SyscallConn()
	if err != nil {
		return err
	}

	inFD := int(src.Fd())
	remaining := length
	off := offset

	for remaining > 0 {
		var sent int
		var callErr error

		err := rc.Write(func(outFD uintptr) bool {
			max := remaining
			if max > int64(^uint(0)>>1) {
				max = int64(^uint(0) >> 1)
			}

			n, err := syscall.Sendfile(int(outFD), inFD, &off, int(max))
			sent = n
			if err == syscall.EAGAIN {
				return false
			}
			callErr = err
			return true
		})
		if err != nil {
			return err
		}
		if sent > 0 {
			remaining -= int64(sent)
			continue
		}
		if callErr != nil {
			if errors.Is(callErr, syscall.EINTR) {
				continue
			}
			return callErr
		}
		return io.ErrUnexpectedEOF
	}
	return nil
}
