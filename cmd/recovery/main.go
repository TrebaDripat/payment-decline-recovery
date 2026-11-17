package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/TrebaDripat/payment-decline-recovery/internal/recovery"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: recovery <serve|worker|snapshot|demo> [flags]")
	}
	command := os.Args[1]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	dbPath := flags.String("db", "recovery.db", "SQLite database path")
	address := flags.String("listen", "127.0.0.1:8080", "HTTP listen address; keep loopback for this demo")
	outcome := flags.String("outcome", "success", "simulated gateway: success, soft, hard")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if command == "demo" {
		return demo()
	}
	if command != "serve" && command != "worker" && command != "snapshot" {
		return errors.New("unknown command")
	}
	if *outcome != "success" && *outcome != "soft" && *outcome != "hard" {
		return errors.New("invalid outcome")
	}
	s, err := recovery.Open(*dbPath)
	if err != nil {
		return err
	}
	defer s.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch command {
	case "snapshot":
		value, err := s.Snapshot(ctx)
		if err != nil {
			return err
		}
		return printJSON(value)
	case "serve":
		h, err := recovery.Handler(s, os.Getenv("RECOVERY_WEBHOOK_SECRET"), time.Now)
		if err != nil {
			return err
		}
		server := &http.Server{Addr: *address, Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
		go func() {
			<-ctx.Done()
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		}()
		log.Printf("simulation-only API listening on %s", *address)
		err = server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case "worker":
		gateway := recovery.SimulatedGateway{Store: s, Outcome: *outcome}
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

func printJSON(v any) error {
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	return e.Encode(v)
}
func demo() error {
	dir, err := os.MkdirTemp("", "payment-recovery-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	s, err := recovery.Open(filepath.Join(dir, "demo.db"))
	if err != nil {
		return err
	}
	defer s.Close()
	ctx := context.Background()
	clock := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	event := recovery.Event{EventID: "delivery-1", InvoiceID: "synthetic-invoice", AttemptID: "initial-attempt", Status: "failed", Code: "insufficient_funds"}
	for i := 0; i < 10; i++ {
		if _, err = s.Ingest(ctx, event, clock); err != nil {
			return err
		}
	}
	event.EventID = "delivery-2"
	if _, err = s.Ingest(ctx, event, clock); err != nil {
		return err
	}
	before, err := s.Snapshot(ctx)
	if err != nil {
		return err
	}
	clock = clock.Add(time.Minute)
	job, err := s.Claim(ctx, clock)
	if err != nil {
		return err
	}
	if job == nil {
		return errors.New("missing scheduled job")
	}
	gateway := recovery.SimulatedGateway{Store: s, Outcome: "success"}
	if _, err = gateway.Recover(ctx, *job); err != nil {
		return err
	}
	// Simulate a process disappearing after the provider commits, before Finish.
	afterCrash, err := s.Snapshot(ctx)
	if err != nil {
		return err
	}
	clock = clock.Add(recovery.LeaseDuration + time.Second)
	if _, err = recovery.Step(ctx, s, gateway, func() time.Time { return clock }); err != nil {
		return err
	}
	final, err := s.Snapshot(ctx)
	if err != nil {
		return err
	}
	if final.ProviderEffects != 1 || final.Attempts != 2 || final.Pending != 0 || final.Invoices[0].State != "paid" {
		return fmt.Errorf("demo invariant failed: %+v", final)
	}
	return printJSON(map[string]any{"data": "synthetic; no money moved", "clock": "fixed logical clock, no waiting", "after_11_deliveries": before, "after_simulated_crash": afterCrash, "after_recovery": final, "invariants_passed": true})
}
