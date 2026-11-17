package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TrebaDripat/payment-decline-recovery/internal/httpserver"
	"github.com/TrebaDripat/payment-decline-recovery/internal/recovery"
	"github.com/TrebaDripat/payment-decline-recovery/internal/simulator"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: recovery <serve|provider|worker|snapshot|inbox|reviews|demo> [flags]")
	}
	command := os.Args[1]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	defaultDB, defaultAddress := "recovery.db", "127.0.0.1:8080"
	if command == "provider" {
		defaultDB = "provider.db"
		defaultAddress = "127.0.0.1:8090"
	}
	dbPath := flags.String("db", defaultDB, "SQLite database path (engine and provider must use different files)")
	address := flags.String("listen", defaultAddress, "HTTP listen address; keep loopback for this demo")
	gatewayURL := flags.String("gateway", "http://127.0.0.1:8090", "simulator HTTP URL")
	outcome := flags.String("outcome", "success", "provider only: success, soft, hard")
	fault := flags.String("fault", "none", "provider only: none, drop_after_commit, timeout_after_commit, unavailable, rate_limit, reject")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	var wrongFlag string
	flags.Visit(func(f *flag.Flag) {
		if command != "provider" && (f.Name == "fault" || f.Name == "outcome") {
			wrongFlag = f.Name
		}
	})
	if wrongFlag != "" {
		return errors.New("-" + wrongFlag + " is only valid for the provider command")
	}
	if command == "demo" {
		return demo()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if command == "provider" {
		options := simulator.Options{Outcome: *outcome, Fault: *fault}
		if err := options.Validate(); err != nil {
			return err
		}
		s, err := simulator.Open(*dbPath)
		if err != nil {
			return err
		}
		defer s.Close()
		handler, err := simulator.Handler(s, options)
		if err != nil {
			return err
		}
		return serve(ctx, *address, handler)
	}
	if command != "serve" && command != "worker" && command != "snapshot" && command != "inbox" && command != "reviews" {
		return errors.New("unknown command")
	}
	s, err := recovery.Open(*dbPath)
	if err != nil {
		return err
	}
	defer s.Close()
	switch command {
	case "snapshot":
		value, err := s.Snapshot(ctx)
		if err != nil {
			return err
		}
		return printJSON(value)
	case "inbox":
		value, err := s.Inbox(ctx)
		if err != nil {
			return err
		}
		return printJSON(value)
	case "reviews":
		value, err := s.Reviews(ctx)
		if err != nil {
			return err
		}
		return printJSON(value)
	case "serve":
		handler, err := recovery.Handler(s, os.Getenv("RECOVERY_WEBHOOK_SECRET"), time.Now)
		if err != nil {
			return err
		}
		return serve(ctx, *address, handler)
	case "worker":
		gateway, err := recovery.NewHTTPGateway(*gatewayURL)
		if err != nil {
			return err
		}
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				_, err := recovery.Step(ctx, s, gateway, time.Now)
				if err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("worker: %v", err)
				}
			}
		}
	}
	return nil
}

func serve(ctx context.Context, address string, handler http.Handler) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
	log.Printf("simulation-only API listening on %s", listener.Addr())
	return httpserver.Serve(ctx, server, listener, 5*time.Second)
}
func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
