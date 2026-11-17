// Package httpserver owns server shutdown so callers can close dependencies only
// after in-flight handlers have drained (or their connections have been closed).
package httpserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

func Serve(ctx context.Context, server *http.Server, listener net.Listener, grace time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), grace)
		defer cancel()
		err := server.Shutdown(shutdown)
		if err != nil {
			_ = server.Close()
		}
		serveErr := <-done
		if !errors.Is(serveErr, http.ErrServerClosed) {
			return errors.Join(err, serveErr)
		}
		return err
	}
}
