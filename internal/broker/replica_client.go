package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/protocol"
)

// maxReplicaResponseBytes bounds response size to avoid unbounded
// allocation from a corrupted or malicious stream.
const maxReplicaResponseBytes = 100 * 1024 * 1024

type ReplicaClient struct {
	addr           string
	clientID       string
	dialTimeout    time.Duration
	requestTimeout time.Duration

	requestMu         sync.Mutex
	mu                sync.Mutex
	conn              net.Conn
	shutdown          bool
	nextCorrelationID int32
}

func NewReplicaClient(addr string, dialTimeout, requestTimeout time.Duration) *ReplicaClient {
	return &ReplicaClient{
		addr:           addr,
		clientID:       "replica-fetcher",
		dialTimeout:    dialTimeout,
		requestTimeout: requestTimeout,
	}
}

func (c *ReplicaClient) Close() error {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (c *ReplicaClient) Shutdown() error {
	c.mu.Lock()
	c.shutdown = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (c *ReplicaClient) ensureConnected() (net.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.shutdown {
		return nil, fmt.Errorf("replica client: shut down")
	}
	if c.conn != nil {
		return c.conn, nil
	}
	conn, err := net.DialTimeout("tcp", c.addr, c.dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("replica client: dial %s: %w", c.addr, err)
	}
	c.conn = conn
	return conn, nil
}

func (c *ReplicaClient) dropConnection(conn net.Conn) {
	c.mu.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	c.mu.Unlock()
	_ = conn.Close()
}

// retries one failed read only replica request once.
func (c *ReplicaClient) sendRequest(apiKey, apiVersion int16, body []byte) ([]byte, error) {
	c.requestMu.Lock()
	defer c.requestMu.Unlock()

	respBody, err := c.sendRequestOnce(apiKey, apiVersion, body)
	if err == nil {
		return respBody, nil
	}
	c.Close()
	return c.sendRequestOnce(apiKey, apiVersion, body)
}

func (c *ReplicaClient) sendRequestOnce(apiKey, apiVersion int16, body []byte) ([]byte, error) {
	conn, err := c.ensureConnected()
	if err != nil {
		return nil, err
	}

	correlationID := atomic.AddInt32(&c.nextCorrelationID, 1)

	headerBuf := protocol.NewEncoder(32)
	clientID := c.clientID
	protocol.EncodeRequestHeader(headerBuf, apiKey, apiVersion, correlationID, &clientID)
	header := headerBuf.Bytes()

	totalSize := len(header) + len(body)
	msg := make([]byte, 4+totalSize)
	binary.BigEndian.PutUint32(msg[0:4], uint32(totalSize))
	copy(msg[4:], header)
	copy(msg[4+len(header):], body)

	if c.requestTimeout > 0 {
		if err := conn.SetDeadline(time.Now().Add(c.requestTimeout)); err != nil {
			c.dropConnection(conn)
			return nil, fmt.Errorf("replica client: set deadline: %w", err)
		}
	}

	if n, err := conn.Write(msg); err != nil {
		c.dropConnection(conn)
		return nil, fmt.Errorf("replica client: write request: %w", err)
	} else if n != len(msg) {
		c.dropConnection(conn)
		return nil, fmt.Errorf("replica client: write request: %w", io.ErrShortWrite)
	}

	respBody, respCorrelationID, err := c.readResponse(conn, apiKey, apiVersion)
	if err != nil {
		c.dropConnection(conn)
		return nil, err
	}
	if respCorrelationID != correlationID {
		c.dropConnection(conn)
		return nil, fmt.Errorf("replica client: correlation ID mismatch: got %d, want %d", respCorrelationID, correlationID)
	}

	return respBody, nil
}

func (c *ReplicaClient) readResponse(conn net.Conn, apiKey, apiVersion int16) ([]byte, int32, error) {
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return nil, 0, fmt.Errorf("replica client: read response size: %w", err)
	}
	size := binary.BigEndian.Uint32(sizeBuf[:])
	if size == 0 || size > maxReplicaResponseBytes {
		return nil, 0, fmt.Errorf("replica client: invalid response size: %d", size)
	}

	msg := make([]byte, size)
	if _, err := io.ReadFull(conn, msg); err != nil {
		return nil, 0, fmt.Errorf("replica client: read response body: %w", err)
	}
	if len(msg) < 4 {
		return nil, 0, fmt.Errorf("replica client: response too short for a correlation ID")
	}

	correlationID, body, err := protocol.ParseResponseHeader(msg, apiKey, apiVersion)
	if err != nil {
		return nil, 0, fmt.Errorf("replica client: parse response header: %w", err)
	}
	return body, correlationID, nil
}

// Fetch sends a Fetch request and returns the decoded response.
func (c *ReplicaClient) Fetch(req *protocol.FetchRequest, apiVersion int16) (*protocol.FetchResponse, error) {
	body := protocol.EncodeFetchRequest(req, apiVersion)
	respBody, err := c.sendRequest(protocol.APIKeyFetch, apiVersion, body)
	if err != nil {
		return nil, err
	}
	return protocol.DecodeFetchResponse(respBody, apiVersion)
}

// OffsetsForLeaderEpoch sends an OffsetsForLeaderEpoch request and
// returns the decoded response.
func (c *ReplicaClient) OffsetsForLeaderEpoch(req *protocol.OffsetsForLeaderEpochRequest, apiVersion int16) (*protocol.OffsetsForLeaderEpochResponse, error) {
	body := protocol.EncodeOffsetsForLeaderEpochRequest(req, apiVersion)
	respBody, err := c.sendRequest(protocol.APIKeyOffsetsForLeaderEpoch, apiVersion, body)
	if err != nil {
		return nil, err
	}
	return protocol.DecodeOffsetsForLeaderEpochResponse(respBody, apiVersion)
}
