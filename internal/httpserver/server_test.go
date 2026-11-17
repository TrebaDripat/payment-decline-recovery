package httpserver

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestShutdownDrainsActiveRequest(t *testing.T) {
	entered, finish := make(chan struct{}), make(chan struct{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-finish
		_, _ = w.Write([]byte("committed"))
	})}
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, server, listener, time.Second) }()
	response := make(chan error, 1)
	go func() {
		r, err := http.Get("http://" + listener.Addr().String())
		if err == nil {
			defer r.Body.Close()
			b, e := io.ReadAll(r.Body)
			err = e
			if string(b) != "committed" {
				err = errors.New("response interrupted")
			}
		}
		response <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		close(finish)
		t.Fatalf("returned before request drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(finish)
	if err := <-response; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDeadlineClosesConnection(t *testing.T) {
	entered, handlerDone := make(chan struct{}), make(chan struct{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(handlerDone) })}
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, server, listener, 30*time.Millisecond) }()
	clientDone := make(chan struct{})
	go func() {
		r, _ := http.Get("http://" + listener.Addr().String())
		if r != nil {
			r.Body.Close()
		}
		close(clientDone)
	}()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("request context not cancelled")
	}
	<-clientDone
}
