package server

import (
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/protocol"
)

// RequestHandler processes decoded Kafka requests.
type RequestHandler interface {
	// Handle processes one request and returns a response body writer.
	//   (body, nil) → send response
	//   (nil, nil)  → suppress response (e.g. acks=0)
	//   (nil, err)  → close connection
	Handle(header protocol.RequestHeader, body []byte) (Response, error)
}

// Server accepts TCP connections and dispatches Kafka protocol requests.
type Server struct {
	listener    net.Listener
	handler     RequestHandler
	logger      *slog.Logger
	maxReqBytes int32

	wg      sync.WaitGroup
	closing chan struct{}
}

// NewServer creates a Server bound to the given address.
func NewServer(addr string, handler RequestHandler, maxReqBytes int32, logger *slog.Logger) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}

	if maxReqBytes <= 0 {
		maxReqBytes = 100 * 1024 * 1024
	}

	s := &Server{
		listener:    ln,
		handler:     handler,
		logger:      logger,
		maxReqBytes: maxReqBytes,
		closing:     make(chan struct{}),
	}
	return s, nil
}

// Addr returns the listener's network address (useful when port is 0).
func (s *Server) Addr() net.Addr { return s.listener.Addr() }

// Serve runs the accept loop. Blocks until Shutdown is called.
func (s *Server) Serve() {
	s.logger.Info("server listening", "addr", s.listener.Addr())
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.closing:
				return // clean shutdown
			default:
			}
			s.logger.Error("accept failed", "err", err)
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			c := newClientConn(conn, s.handler, s.maxReqBytes, s.logger)
			c.serve(s.closing)
		}()
	}
}

// Shutdown closes the listener and waits for all connections to drain.
func (s *Server) Shutdown() {
	close(s.closing)
	s.listener.Close()
	s.wg.Wait()
	s.logger.Info("server stopped")
}
