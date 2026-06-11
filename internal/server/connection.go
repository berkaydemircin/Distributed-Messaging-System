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
	writer      *bufio.Writer
	handler     RequestHandler
	maxReqBytes int32
	logger      *slog.Logger
}

func newClientConn(conn net.Conn, handler RequestHandler, maxReqBytes int32, logger *slog.Logger) *clientConn {
	return &clientConn{
		conn:        conn,
		reader:      bufio.NewReaderSize(conn, 64*1024),
		writer:      bufio.NewWriterSize(conn, 64*1024),
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

		respBody, err := c.handler.Handle(header, body)
		if err != nil {
			c.logger.Warn("handler error, closing connection",
				"api", header.APIKey, "err", err, "remote", c.conn.RemoteAddr())
			return
		}

		if respBody == nil {
			continue // no response ie. acks=0
		}

		if err := c.writeResponse(header, respBody); err != nil {
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
func (c *clientConn) writeResponse(reqHeader protocol.RequestHeader, body []byte) error {
	respHeaderVersion := protocol.ResponseHeaderVersion(reqHeader.APIKey, reqHeader.APIVersion)

	// Build the full response payload (header + body)
	headerSize := 4 // correlation_id
	if respHeaderVersion >= 1 {
		headerSize += 1 // tagged_fields (just uvarint 0 = 1 byte)
	}

	totalSize := headerSize + len(body)

	// Write message_size prefix
	var sizeBuf [4]byte
	binary.BigEndian.PutUint32(sizeBuf[:], uint32(totalSize))
	if _, err := c.writer.Write(sizeBuf[:]); err != nil {
		return err
	}

	// Write response header
	var corrBuf [4]byte
	binary.BigEndian.PutUint32(corrBuf[:], uint32(reqHeader.CorrelationID))
	if _, err := c.writer.Write(corrBuf[:]); err != nil {
		return err
	}
	if respHeaderVersion >= 1 {
		if _, err := c.writer.Write([]byte{0}); err != nil { // empty tagged fields
			return err
		}
	}

	// Write body
	if _, err := c.writer.Write(body); err != nil {
		return err
	}

	return c.writer.Flush()
}

func isNormalClose(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if opErr, ok := errors.AsType[*net.OpError](err); ok {
		return opErr.Err.Error() == "use of closed network connection"
	}
	return false
}
