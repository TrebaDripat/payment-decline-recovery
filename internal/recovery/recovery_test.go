package recovery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var start = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
var ctx = context.Background()

func openTest(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func testStore(t *testing.T) *Store { return openTest(t, filepath.Join(t.TempDir(), "test.db")) }
func event(id string) Event {
	return Event{EventID: "event-" + id, InvoiceID: "invoice-" + id, AttemptID: "attempt-" + id, Status: "failed", Code: "insufficient_funds"}
}
func ingest(t *testing.T, s *Store, e Event) {
	t.Helper()
	if _, err := s.Ingest(ctx, e, start); err != nil {
		t.Fatal(err)
	}
}
func snapshot(t *testing.T, s *Store) Snapshot {
	t.Helper()
	v, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func claim(t *testing.T, s *Store, now time.Time) *Job {
	t.Helper()
	j, err := s.Claim(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if j == nil {
		t.Fatal("no job")
	}
	return j
}

func TestDeliveryAndAttemptDeduplication(t *testing.T) {
	s := testStore(t)
	e := event("a")
	ingest(t, s, e)
	for i := 0; i < 20; i++ {
		duplicate, err := s.Ingest(ctx, e, start)
		if err != nil || !duplicate {
			t.Fatalf("duplicate=%v err=%v", duplicate, err)
		}
	}
	e.EventID = "different-event-same-attempt"
	ingest(t, s, e)
	v := snapshot(t, s)
	if v.Events != 2 || v.Attempts != 1 || v.Jobs != 1 {
		t.Fatalf("%+v", v)
	}
}
func TestConflictingEventsRollback(t *testing.T) {
	for _, kind := range []string{"same-delivery-new-payload", "same-attempt-new-invoice", "same-attempt-new-outcome", "unknown-attempt-existing-invoice"} {
		t.Run(kind, func(t *testing.T) {
			s := testStore(t)
			e := event("a")
			ingest(t, s, e)
			switch kind {
			case "same-delivery-new-payload":
				e.InvoiceID = "other"
			case "same-attempt-new-invoice":
				e.EventID = "other"
				e.InvoiceID = "other"
			case "same-attempt-new-outcome":
				e.EventID = "other"
				e.Status = "succeeded"
				e.Code = ""
			case "unknown-attempt-existing-invoice":
				e.EventID = "other"
				e.AttemptID = "unknown"
			}
			if _, err := s.Ingest(ctx, e, start); !errors.Is(err, ErrConflict) {
				t.Fatalf("expected conflict, got %v", err)
			}
			v := snapshot(t, s)
			if v.Events != 1 || v.Attempts != 1 || v.Jobs != 1 || len(v.Invoices) != 1 {
				t.Fatalf("partial write: %+v", v)
			}
		})
	}
}
func TestTerminalInitialEventsNeverRetry(t *testing.T) {
	for _, code := range []string{"lost_card", "do_not_honor", ""} {
		t.Run(code, func(t *testing.T) {
			s := testStore(t)
			e := event("a")
			e.Code = code
			if code == "" {
				e.Status = "succeeded"
			}
			ingest(t, s, e)
			if v := snapshot(t, s); v.Jobs != 0 {
				t.Fatalf("%+v", v)
			}
		})
	}
}
func TestCrashAfterProviderCommitAndFencing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")
	s := openTest(t, path)
	ingest(t, s, event("a"))
	now := start.Add(time.Minute)
	old := claim(t, s, now)
	gateway := SimulatedGateway{s, "success"}
	receipt, err := gateway.Recover(ctx, *old)
	if err != nil {
		t.Fatal(err)
	}
	// A fresh connection represents restart; the logical provider effect has committed.
	restarted := openTest(t, path)
	later := now.Add(LeaseDuration + time.Millisecond)
	current := claim(t, restarted, later)
	if old.ID != current.ID || old.Generation == current.Generation {
		t.Fatal("wrong lease identity")
	}
	if err = s.Finish(ctx, *old, receipt, later); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("old owner committed: %v", err)
	}
	if err = s.Release(ctx, *old, later); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("old owner released: %v", err)
	}
	again, err := (SimulatedGateway{restarted, "hard"}).Recover(ctx, *current)
	if err != nil {
		t.Fatal(err)
	}
	if again != receipt {
		t.Fatal("provider changed a committed outcome")
	}
	if err = restarted.Finish(ctx, *current, again, later); err != nil {
		t.Fatal(err)
	}
	v := snapshot(t, restarted)
	if v.ProviderEffects != 1 || v.Attempts != 2 || v.Pending != 0 || v.Invoices[0].State != "paid" {
		t.Fatalf("%+v", v)
	}
	// A late duplicate decline cannot reopen a paid invoice.
	e := event("a")
	e.EventID = "late-delivery"
	ingest(t, restarted, e)
	if v = snapshot(t, restarted); v.Pending != 0 || v.Invoices[0].State != "paid" {
		t.Fatalf("%+v", v)
	}
}
func TestRetryBudgetAndSchedule(t *testing.T) {
	s := testStore(t)
	ingest(t, s, event("a"))
	now := start
	for ordinal := 1; ordinal <= MaxRetries; ordinal++ {
		delay := time.Duration(1<<uint(ordinal-1)) * time.Minute
		if j, err := s.Claim(ctx, now.Add(delay-time.Millisecond)); err != nil || j != nil {
			t.Fatalf("job ran before due: %v %v", j, err)
		}
		now = now.Add(delay)
		ok, err := Step(ctx, s, SimulatedGateway{s, "soft"}, func() time.Time { return now })
		if err != nil || !ok {
			t.Fatalf("step: %v %v", ok, err)
		}
	}
	v := snapshot(t, s)
	if v.ProviderEffects != 3 || v.Attempts != 4 || v.Pending != 0 || v.Invoices[0].State != "exhausted" {
		t.Fatalf("%+v", v)
	}
}

type ambiguousGateway struct{ g SimulatedGateway }

func (g ambiguousGateway) Recover(c context.Context, j Job) (Receipt, error) {
	_, err := g.g.Recover(c, j)
	if err != nil {
		return Receipt{}, err
	}
	return Receipt{}, errors.New("response lost after provider commit")
}
func TestAmbiguousTimeoutReusesSameKey(t *testing.T) {
	s := testStore(t)
	ingest(t, s, event("a"))
	now := start.Add(time.Minute)
	if _, err := Step(ctx, s, ambiguousGateway{SimulatedGateway{s, "success"}}, func() time.Time { return now }); err == nil {
		t.Fatal("expected transport error")
	}
	v := snapshot(t, s)
	if v.Attempts != 1 || v.ProviderEffects != 1 || v.Jobs != 1 {
		t.Fatalf("%+v", v)
	}
	now = now.Add(3 * time.Second)
	if _, err := Step(ctx, s, SimulatedGateway{s, "success"}, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	if v = snapshot(t, s); v.Attempts != 2 || v.ProviderEffects != 1 || v.Pending != 0 {
		t.Fatalf("%+v", v)
	}
}
func TestConcurrentWebhookConnections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.db")
	stores := []*Store{openTest(t, path), openTest(t, path), openTest(t, path), openTest(t, path)}
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := event("a")
			e.EventID = fmt.Sprintf("delivery-%d", i)
			_, err := stores[i%4].Ingest(ctx, e, start)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	v := snapshot(t, stores[0])
	if v.Events != 40 || v.Jobs != 1 || v.Attempts != 1 {
		t.Fatalf("%+v", v)
	}
}
func TestWorkerProcess(t *testing.T) {
	path := os.Getenv("RECOVERY_TEST_WORKER_DB")
	if path == "" {
		t.Skip("helper process only")
	}
	s := openTest(t, path)
	now := start.Add(time.Minute)
	for {
		worked, err := Step(ctx, s, SimulatedGateway{s, "success"}, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			return
		}
	}
}
func TestMultipleWorkerProcesses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "processes.db")
	s := openTest(t, path)
	for i := 0; i < 24; i++ {
		ingest(t, s, event(fmt.Sprint(i)))
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command(os.Args[0], "-test.run=^TestWorkerProcess$", "-test.v")
			cmd.Env = append(os.Environ(), "RECOVERY_TEST_WORKER_DB="+path)
			out, err := cmd.CombinedOutput()
			if err != nil {
				errs <- fmt.Errorf("%w: %s", err, out)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	v := snapshot(t, s)
	if v.ProviderEffects != 24 || v.Attempts != 48 || v.Pending != 0 {
		t.Fatalf("%+v", v)
	}
}
