package simulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TrebaDripat/payment-decline-recovery/internal/recovery"
	"github.com/TrebaDripat/payment-decline-recovery/internal/simulator"
)

var clockStart = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
var background = context.Background()

func openEngine(t *testing.T, path string) *recovery.Store {
	t.Helper()
	s, err := recovery.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func openProvider(t *testing.T, path string) *simulator.Store {
	t.Helper()
	s, err := simulator.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func seed(t *testing.T, s *recovery.Store) {
	t.Helper()
	_, err := s.Ingest(background, recovery.Event{EventID: "event-a", InvoiceID: "invoice-a", AttemptID: "initial-a", Status: "failed", Code: "insufficient_funds"}, clockStart)
	if err != nil {
		t.Fatal(err)
	}
}
func client(t *testing.T, url string) *recovery.HTTPGateway {
	t.Helper()
	g, err := recovery.NewHTTPGateway(url)
	if err != nil {
		t.Fatal(err)
	}
	return g
}
func snapshot(t *testing.T, s *recovery.Store) recovery.Snapshot {
	t.Helper()
	value, err := s.Snapshot(background)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func effects(t *testing.T, p *simulator.Store) int {
	t.Helper()
	n, err := p.Count(background)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRealHTTPDropAfterCommitReconciles(t *testing.T) {
	dir := t.TempDir()
	engine := openEngine(t, filepath.Join(dir, "engine.db"))
	provider := openProvider(t, filepath.Join(dir, "provider.db"))
	seed(t, engine)
	handler, err := simulator.Handler(provider, simulator.Options{Outcome: "success", Fault: "drop_after_commit"})
	if err != nil {
		t.Fatal(err)
	}
	var posts, lookups atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			posts.Add(1)
		} else {
			lookups.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	gateway := client(t, server.URL)
	now := clockStart.Add(time.Minute)
	if _, err = recovery.Step(background, engine, gateway, func() time.Time { return now }); err == nil {
		t.Fatal("drop did not lose response")
	}
	if effects(t, provider) != 1 || snapshot(t, engine).Attempts != 1 {
		t.Fatal("crash window not exposed")
	}
	now = now.Add(3 * time.Second)
	if _, err = recovery.Step(background, engine, gateway, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	final := snapshot(t, engine)
	if final.Pending != 0 || final.Attempts != 2 || final.Invoices[0].State != "paid" || effects(t, provider) != 1 || posts.Load() != 1 || lookups.Load() != 1 {
		t.Fatalf("bad reconciliation: %+v POST=%d GET=%d", final, posts.Load(), lookups.Load())
	}
}

func TestHTTPFaultClassification(t *testing.T) {
	for _, fault := range []string{"unavailable", "rate_limit", "reject"} {
		t.Run(fault, func(t *testing.T) {
			dir := t.TempDir()
			engine := openEngine(t, filepath.Join(dir, "engine.db"))
			provider := openProvider(t, filepath.Join(dir, "provider.db"))
			seed(t, engine)
			handler, err := simulator.Handler(provider, simulator.Options{Outcome: "success", Fault: fault})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			gateway := client(t, server.URL)
			now := clockStart.Add(time.Minute)
			_, err = recovery.Step(background, engine, gateway, func() time.Time { return now })
			final := snapshot(t, engine)
			if fault == "reject" {
				if err != nil || final.ReviewJobs != 1 || final.Pending != 0 {
					t.Fatalf("%+v %v", final, err)
				}
			} else {
				if err == nil || final.Pending != 1 || final.ReviewJobs != 0 {
					t.Fatalf("%+v %v", final, err)
				}
			}
			if effects(t, provider) != 0 || final.Attempts != 1 {
				t.Fatal("transport fault became a payment attempt")
			}
		})
	}
}

func TestHTTPDeadlineAfterProviderCommit(t *testing.T) {
	provider := openProvider(t, filepath.Join(t.TempDir(), "provider.db"))
	handler, err := simulator.Handler(provider, simulator.Options{Outcome: "success", Fault: "timeout_after_commit", Delay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	gateway := client(t, server.URL)
	callCtx, cancel := context.WithTimeout(background, 250*time.Millisecond)
	defer cancel()
	job := recovery.Job{ID: "timeout-key", InvoiceID: "invoice-a"}
	if _, err = gateway.Recover(callCtx, job); err == nil {
		t.Fatal("expected network deadline")
	}
	if effects(t, provider) != 1 {
		t.Fatal("provider did not commit before the response deadline")
	}
	receipt, err := gateway.Lookup(background, job)
	if err != nil || receipt.Status != "succeeded" {
		t.Fatalf("lost receipt: %+v %v", receipt, err)
	}
}

func TestProviderReceiptsSurviveRestartAndConcurrentClients(t *testing.T) {
	path := filepath.Join(t.TempDir(), "provider.db")
	provider := openProvider(t, path)
	handler, err := simulator.Handler(provider, simulator.Options{Outcome: "success", Fault: "none"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	gateway := client(t, server.URL)
	job := recovery.Job{ID: "stable-key", InvoiceID: "invoice-a"}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := gateway.Recover(background, job)
			if err == nil && r.Status != "succeeded" {
				err = fmt.Errorf("unexpected receipt: %+v", r)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if effects(t, provider) != 1 {
		t.Fatal("duplicate provider effects")
	}
	server.Close()
	provider.Close()
	restarted := openProvider(t, path)
	handler, err = simulator.Handler(restarted, simulator.Options{Outcome: "hard", Fault: "none"})
	if err != nil {
		t.Fatal(err)
	}
	server = httptest.NewServer(handler)
	defer server.Close()
	gateway = client(t, server.URL)
	receipt, err := gateway.Lookup(background, job)
	if err != nil || receipt.Status != "succeeded" {
		t.Fatalf("lost committed outcome: %+v %v", receipt, err)
	}
	job.InvoiceID = "different-invoice"
	_, err = gateway.Recover(background, job)
	var providerErr *recovery.GatewayError
	if !errors.As(err, &providerErr) || providerErr.Kind != "rejected" || effects(t, restarted) != 1 {
		t.Fatalf("reused key accepted: %v", err)
	}
}

func TestHTTPClientRejectsMalformedReceiptsAndRedirects(t *testing.T) {
	for _, mode := range []string{"malformed", "extra_json", "oversized", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			targetCalls := atomic.Int32{}
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls.Add(1) }))
			defer target.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "malformed":
					_, _ = io.WriteString(w, "{")
				case "extra_json":
					_, _ = io.WriteString(w, `{"attempt_id":"a","status":"succeeded"} {}`)
				case "oversized":
					_, _ = w.Write(make([]byte, 65537))
				case "redirect":
					http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
				}
			}))
			defer server.Close()
			_, err := client(t, server.URL).Recover(background, recovery.Job{ID: "a", InvoiceID: "i"})
			if err == nil || targetCalls.Load() != 0 {
				t.Fatal("invalid response accepted or redirect followed")
			}
		})
	}
}

// This helper is executed in separate OS processes by the crash integration test.
func TestRecoveryWorkerHelper(t *testing.T) {
	path := os.Getenv("RECOVERY_HTTP_TEST_DB")
	if path == "" {
		t.Skip("worker subprocess only")
	}
	nowMS, err := strconv.ParseInt(os.Getenv("RECOVERY_HTTP_TEST_NOW"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	engine := openEngine(t, path)
	gateway := client(t, os.Getenv("RECOVERY_HTTP_TEST_GATEWAY"))
	now := time.UnixMilli(nowMS)
	if _, err = recovery.Step(background, engine, gateway, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
}

func TestKilledWorkerReconcilesDurableHTTPReceipt(t *testing.T) {
	dir := t.TempDir()
	enginePath := filepath.Join(dir, "engine.db")
	engine := openEngine(t, enginePath)
	provider := openProvider(t, filepath.Join(dir, "provider.db"))
	seed(t, engine)
	handler, err := simulator.Handler(provider, simulator.Options{Outcome: "success", Fault: "timeout_after_commit", Delay: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	var posts, lookups atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			posts.Add(1)
		} else {
			lookups.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	now := clockStart.Add(time.Minute)
	command := func(instant time.Time) *exec.Cmd {
		c := exec.Command(os.Args[0], "-test.run=^TestRecoveryWorkerHelper$", "-test.v")
		c.Env = append(os.Environ(), "RECOVERY_HTTP_TEST_DB="+enginePath, "RECOVERY_HTTP_TEST_GATEWAY="+server.URL, "RECOVERY_HTTP_TEST_NOW="+strconv.FormatInt(instant.UnixMilli(), 10))
		return c
	}
	worker := command(now)
	if err = worker.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if worker.ProcessState == nil {
			_ = worker.Process.Kill()
			_ = worker.Wait()
		}
	}()
	deadline := time.Now().Add(4 * time.Second)
	for effects(t, provider) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("provider did not commit before kill")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if snapshot(t, engine).Attempts != 1 {
		t.Fatal("worker finished before fault injection")
	}
	if err = worker.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = worker.Wait(); err == nil {
		t.Fatal("worker was not terminated")
	}
	// Advance only the engine's logical lease clock. The provider is real HTTP,
	// persisted its receipt independently, and never shares the worker process.
	restarted := command(now.Add(recovery.LeaseDuration + time.Second))
	if output, err := restarted.CombinedOutput(); err != nil {
		t.Fatalf("restart: %v %s", err, output)
	}
	final := snapshot(t, engine)
	if final.Pending != 0 || final.Attempts != 2 || final.Invoices[0].State != "paid" || effects(t, provider) != 1 || posts.Load() != 1 || lookups.Load() != 1 {
		b, _ := json.Marshal(final)
		t.Fatalf("duplicate or lost result: %s POST=%d GET=%d", b, posts.Load(), lookups.Load())
	}
}
