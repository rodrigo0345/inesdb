package server

import (
	"context"
	"errors"
	"net"

	"github.com/rodrigo0345/omag/src/engine"
	"github.com/rodrigo0345/omag/pkg/pkglog"
)

// Server accepts incoming psql connections and spawns a Session per connection.
type Server struct {
	db engine.Database
}

// New creates a pgwire server backed by the given database engine.
func New(db engine.Database) *Server {
	return &Server{db: db}
}

// RunConn runs a single session on the given connection. Useful for testing.
func (srv *Server) RunConn(conn net.Conn) error {
	sess := newSession(conn, srv.db)
	return sess.Run()
}

// Serve accepts connections from ln until ctx is cancelled.
func (srv *Server) Serve(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return nil
			}
			pkglog.Warn("[pgserver] accept error: %v", err)
			continue
		}
		go func(c net.Conn) {
			sess := newSession(c, srv.db)
			if err := sess.Run(); err != nil {
				pkglog.Debug("[pgserver] session ended: %v", err)
			}
		}(conn)
	}
}
