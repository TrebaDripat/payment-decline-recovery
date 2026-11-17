package main

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/TrebaDripat/payment-decline-recovery/internal/recovery"
	"github.com/TrebaDripat/payment-decline-recovery/internal/simulator"
)

func demo() error {
	dir, err := os.MkdirTemp("", "payment-recovery-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	engine, err := recovery.Open(filepath.Join(dir, "engine.db"))
	if err != nil {
		return err
	}
	defer engine.Close()
	provider, err := simulator.Open(filepath.Join(dir, "provider.db"))
	if err != nil {
		return err
	}
	defer provider.Close()
	handler, err := simulator.Handler(provider, simulator.Options{Outcome: "success", Fault: "drop_after_commit"})
	if err != nil {
		return err
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	gateway, err := recovery.NewHTTPGateway(server.URL)
	if err != nil {
		return err
	}
	ctx := context.Background()
	clock := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	event := recovery.Event{EventID: "delivery-1", InvoiceID: "synthetic-invoice", AttemptID: "initial-attempt", Status: "failed", Code: "insufficient_funds"}
	for i := 0; i < 10; i++ {
		if _, err = engine.Ingest(ctx, event, clock); err != nil {
			return err
		}
	}
	event.EventID = "delivery-2"
	if _, err = engine.Ingest(ctx, event, clock); err != nil {
		return err
	}
	before, err := engine.Snapshot(ctx)
	if err != nil {
		return err
	}
	clock = clock.Add(time.Minute)
	if _, err = recovery.Step(ctx, engine, gateway, func() time.Time { return clock }); err == nil {
		return errors.New("expected dropped provider response")
	}
	unknown, err := engine.Snapshot(ctx)
	if err != nil {
		return err
	}
	committed, err := provider.Count(ctx)
	if err != nil {
		return err
	}
	clock = clock.Add(3 * time.Second)
	if _, err = recovery.Step(ctx, engine, gateway, func() time.Time { return clock }); err != nil {
		return err
	}
	final, err := engine.Snapshot(ctx)
	if err != nil {
		return err
	}
	effects, err := provider.Count(ctx)
	if err != nil {
		return err
	}
	if effects != 1 || committed != 1 || final.Attempts != 2 || final.Pending != 0 || final.Invoices[0].State != "paid" {
		return fmt.Errorf("demo invariant failed: %+v; effects=%d", final, effects)
	}
	return printJSON(map[string]any{"data": "synthetic; no money moved", "transport": "real loopback HTTP; separate engine and provider SQLite files", "after_11_deliveries": before, "after_lost_response": unknown, "provider_effects_after_lost_response": committed, "after_reconciliation": final, "provider_effects_after_reconciliation": effects, "invariants_passed": true})
}
