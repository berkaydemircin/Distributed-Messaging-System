package server

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/protocol"
)

type clientConn struct {
	conn        net.Conn
	reader      *bufio.Reader
	handler     RequestHandler
	maxReqBytes int32
	logger      *slog.Logger
}

func newClientConn(conn net.Conn, handler RequestHandler, maxReqBytes int32, logger *slog.Logger) *clientConn {
	return &clientConn{
		conn:        conn,
		reader:      bufio.NewReaderSize(conn, 64*1024),
		handler:     handler,
		maxReqBytes: maxReqBytes,
		logger:      logger,
	}
}

func (c *clientConn) serve(closing <-chan struct{}) {
	defer c.conn.Close()

	for {
		// check shutdown
		select {
		case <-closing:
			return
		default:
		}

		msg, err := c.readMessage()
		if err != nil {
			if !isNormalClose(err) {
				c.logger.Debug("read error", "remote", c.conn.RemoteAddr(), "err", err)
			}
			return
		}

		header, body, err := protocol.ParseRequestHeader(msg)
		if err != nil {
			c.logger.Warn("bad request header", "remote", c.conn.RemoteAddr(), "err", err)
			return
		}

		c.logger.Debug("request",
			"api", header.APIKey,
			"version", header.APIVersion,
			"correlation", header.CorrelationID,
			"remote", c.conn.RemoteAddr())

		resp, err := c.handler.Handle(header, body)
		if err != nil {
			c.logger.Warn("handler error, closing connection",
				"api", header.APIKey, "err", err, "remote", c.conn.RemoteAddr())
			return
		}

		if resp == nil {
			continue // no response ie. acks=0
		}

		if err := c.writeResponse(header, resp); err != nil {
			c.logger.Warn("write error", "remote", c.conn.RemoteAddr(), "err", err)
			return
		}
	}
}

// readMessage reads a length-prefixed Kafka message from the wire.
// Wire format: [message_size: int32][message_bytes]
func (c *clientConn) readMessage() ([]byte, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(c.reader, sizeBuf[:]); err != nil {
		return nil, err
	}

	size := int32(binary.BigEndian.Uint32(sizeBuf[:]))
	if size <= 0 || size > c.maxReqBytes {
		return nil, fmt.Errorf("invalid request size: %d (max %d)", size, c.maxReqBytes)
	}

	msg := make([]byte, size)
	if _, err := io.ReadFull(c.reader, msg); err != nil {
		return nil, fmt.Errorf("read message body: %w", err)
	}
	return msg, nil
}

// writeResponse writes a framed Kafka response.
// Wire format: [message_size: int32][correlation_id: int32][body]
// For flexible response headers (v1): [correlation_id][tagged_fields][body]
func (c *clientConn) writeResponse(reqHeader protocol.RequestHeader, resp Response) error {
	respHeaderVersion := protocol.ResponseHeaderVersion(reqHeader.APIKey, reqHeader.APIVersion)

	headerSize := 4 // correlation_id
	if respHeaderVersion >= 1 {
		headerSize += 1 // tagged_fields (just uvarint 0 = 1 byte)
	}

	bodySize := resp.BodySize()
	totalSize := int64(headerSize) + bodySize
	if totalSize > int64(^uint32(0)) {
		return fmt.Errorf("response too large: %d", totalSize)
	}

	var prefix [9]byte // size(4) + correlation_id(4) + opt tagged_fields(1)
	binary.BigEndian.PutUint32(prefix[0:4], uint32(totalSize))
	binary.BigEndian.PutUint32(prefix[4:8], uint32(reqHeader.CorrelationID))

	prefixLen := 8
	if respHeaderVersion >= 1 {
		prefix[8] = 0
		prefixLen = 9
	}

	// Fast path for ordinary byte responses: one vectored write for prefix+body.
	if body, ok := resp.(BytesResponse); ok {
		buffers := net.Buffers{prefix[:prefixLen]}
		if len(body) > 0 {
			buffers = append(buffers, []byte(body))
		}
		_, err := buffers.WriteTo(c.conn)
		return err
	}

	w := NewResponseWriter(c.conn)
	if err := w.WriteBytes(prefix[:prefixLen]); err != nil {
		return err
	}
	return resp.WriteBodyTo(w)
}

func isNormalClose(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Err.Error() == "use of closed network connection"
	}
	return false
}
