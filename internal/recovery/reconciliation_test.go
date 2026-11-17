package recovery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

type scriptedGateway struct {
	recover func(context.Context, Job) (Receipt, error)
	lookup  func(context.Context, Job) (Receipt, error)
}

func (g scriptedGateway) Recover(c context.Context, j Job) (Receipt, error) { return g.recover(c, j) }
func (g scriptedGateway) Lookup(c context.Context, j Job) (Receipt, error)  { return g.lookup(c, j) }

func TestUnknownOutcomeUsesLookupWithoutSecondRecover(t *testing.T) {
	s := testStore(t)
	ingest(t, s, event("a"))
	now := start.Add(time.Minute)
	recoverCalls, lookupCalls := 0, 0
	gateway := scriptedGateway{
		recover: func(c context.Context, j Job) (Receipt, error) {
			recoverCalls++
			_, err := (SimulatedGateway{s, "success"}).Recover(c, j)
			if err != nil {
				t.Fatal(err)
			}
			return Receipt{}, errors.New("response lost")
		},
		lookup: func(c context.Context, j Job) (Receipt, error) {
			lookupCalls++
			return (SimulatedGateway{s, "success"}).Lookup(c, j)
		},
	}
	if _, err := Step(ctx, s, gateway, func() time.Time { return now }); err == nil {
		t.Fatal("expected unknown result")
	}
	now = now.Add(3 * time.Second)
	if _, err := Step(ctx, s, gateway, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	if recoverCalls != 1 || lookupCalls != 1 || snapshot(t, s).ProviderEffects != 1 {
		t.Fatalf("recover=%d lookup=%d", recoverCalls, lookupCalls)
	}
}

func TestUnknownResultExpiresIntoReview(t *testing.T) {
	s := testStore(t)
	ingest(t, s, event("a"))
	now := start.Add(time.Minute)
	first := now
	recoverCalls, lookupCalls := 0, 0
	gateway := scriptedGateway{
		recover: func(context.Context, Job) (Receipt, error) {
			recoverCalls++
			return Receipt{}, errors.New("transport unknown")
		},
		lookup: func(context.Context, Job) (Receipt, error) {
			lookupCalls++
			return Receipt{}, errors.New("lookup unavailable")
		},
	}
	_, _ = Step(ctx, s, gateway, func() time.Time { return now })
	now = now.Add(3 * time.Second)
	_, _ = Step(ctx, s, gateway, func() time.Time { return now })
	now = first.Add(ResolutionWindow)
	if _, err := Step(ctx, s, gateway, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	state := snapshot(t, s)
	reviews, err := s.Reviews(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recoverCalls != 1 || lookupCalls != 1 || state.Attempts != 1 || state.ReviewJobs != 1 || state.Pending != 0 || len(reviews) != 1 || reviews[0].Reason != "resolution_window_elapsed" {
		t.Fatalf("unexpected expiry: %+v %+v calls=%d,%d", state, reviews, recoverCalls, lookupCalls)
	}
	if worked, err := Step(ctx, s, gateway, func() time.Time { return now.Add(time.Hour) }); err != nil || worked {
		t.Fatal("review job ran again")
	}
}

func TestAuthoritativeNotFoundReusesKey(t *testing.T) {
	s := testStore(t)
	ingest(t, s, event("a"))
	now := start.Add(time.Minute)
	keys := []string{}
	gateway := scriptedGateway{
		recover: func(c context.Context, j Job) (Receipt, error) {
			keys = append(keys, j.ID)
			if len(keys) == 1 {
				return Receipt{}, errors.New("request never reached provider")
			}
			return (SimulatedGateway{s, "success"}).Recover(c, j)
		},
		lookup: func(context.Context, Job) (Receipt, error) { return Receipt{}, ErrReceiptNotFound },
	}
	_, _ = Step(ctx, s, gateway, func() time.Time { return now })
	now = now.Add(3 * time.Second)
	if _, err := Step(ctx, s, gateway, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != keys[1] || snapshot(t, s).ProviderEffects != 1 {
		t.Fatalf("keys=%v", keys)
	}
}

func TestPermanentRejectionAndInvalidReceiptNeedReview(t *testing.T) {
	for _, kind := range []string{"rejected", "invalid_receipt"} {
		t.Run(kind, func(t *testing.T) {
			s := testStore(t)
			ingest(t, s, event("a"))
			now := start.Add(time.Minute)
			gateway := scriptedGateway{recover: func(context.Context, Job) (Receipt, error) {
				if kind == "rejected" {
					return Receipt{}, &GatewayError{Kind: "rejected"}
				}
				return Receipt{AttemptID: "bad", Status: "pending"}, nil
			}}
			if _, err := Step(ctx, s, gateway, func() time.Time { return now }); err != nil {
				t.Fatal(err)
			}
			if state := snapshot(t, s); state.ReviewJobs != 1 || state.Pending != 0 || state.Attempts != 1 {
				t.Fatalf("%+v", state)
			}
		})
	}
}

func TestTemporaryRejectionHonorsRetryAfter(t *testing.T) {
	s := testStore(t)
	ingest(t, s, event("a"))
	now := start.Add(time.Minute)
	calls := 0
	gateway := scriptedGateway{recover: func(c context.Context, j Job) (Receipt, error) {
		calls++
		if calls == 1 {
			return Receipt{}, &GatewayError{Kind: "temporary", RetryAfter: 20 * time.Second}
		}
		return (SimulatedGateway{s, "success"}).Recover(c, j)
	}, lookup: func(context.Context, Job) (Receipt, error) {
		t.Fatal("known pre-acceptance rejection should not require lookup")
		return Receipt{}, nil
	}}
	_, _ = Step(ctx, s, gateway, func() time.Time { return now })
	now = now.Add(19 * time.Second)
	if worked, err := Step(ctx, s, gateway, func() time.Time { return now }); err != nil || worked {
		t.Fatal("retried early")
	}
	now = now.Add(time.Second)
	if _, err := Step(ctx, s, gateway, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}

func TestConflictPersistsAndFencesAnActiveLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.db")
	s := openTest(t, path)
	original := event("a")
	ingest(t, s, original)
	lease := claim(t, s, start.Add(time.Minute))
	conflict := original
	conflict.Status = "succeeded"
	conflict.Code = ""
	first, err := s.Ingest(ctx, conflict, start.Add(time.Minute))
	if err != nil || first.Status != "needs_review" {
		t.Fatalf("%+v %v", first, err)
	}
	second, err := s.Ingest(ctx, conflict, start.Add(2*time.Minute))
	if err != nil || !second.DuplicateDelivery || second.Status != "needs_review" {
		t.Fatalf("%+v %v", second, err)
	}
	if err := s.CheckLease(ctx, *lease, start.Add(time.Minute)); !errors.Is(err, ErrStaleLease) {
		t.Fatal("paused lease can dispatch", err)
	}
	reopened := openTest(t, path)
	entries, err := reopened.Inbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Deliveries != 2 || entries[0].Status != "needs_review" {
		t.Fatalf("%+v", entries)
	}
	if state := snapshot(t, reopened); state.Attempts != 1 || state.ReviewJobs != 1 || state.Pending != 0 {
		t.Fatalf("%+v", state)
	}
}

func TestWebhookPauseDuringLookupStopsRedispatch(t *testing.T) {
	s := testStore(t)
	ingest(t, s, event("a"))
	now := start.Add(time.Minute)
	old := claim(t, s, now)
	if err := s.Release(ctx, *old, now); err != nil {
		t.Fatal(err)
	}
	now = now.Add(3 * time.Second)
	gateway := scriptedGateway{
		lookup: func(context.Context, Job) (Receipt, error) {
			e := event("a")
			e.EventID = "external-payment"
			e.AttemptID = "external-attempt"
			e.Status = "succeeded"
			e.Code = ""
			_, err := s.Ingest(ctx, e, now)
			if err != nil {
				t.Fatal(err)
			}
			return Receipt{}, ErrReceiptNotFound
		},
		recover: func(context.Context, Job) (Receipt, error) { t.Fatal("redispatched paused job"); return Receipt{}, nil },
	}
	if _, err := Step(ctx, s, gateway, func() time.Time { return now }); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("got %v", err)
	}
}

func TestLegacyMigrationPreservesReceiptsAndPausesUnfinishedDeliveries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(legacySchema); err != nil {
		t.Fatal(err)
	}
	e := event("a")
	payload, _ := json.Marshal(e)
	statements := []struct {
		q    string
		args []any
	}{
		{"INSERT INTO invoices VALUES(?,?,?)", []any{e.InvoiceID, e.AttemptID, "retry_scheduled"}},
		{"INSERT INTO attempts VALUES(?,?,?,?,?)", []any{e.AttemptID, e.InvoiceID, 0, e.Status, e.Code}},
		{"INSERT INTO events VALUES(?,?,?)", []any{e.EventID, string(payload), start.UnixMilli()}},
		{"INSERT INTO jobs(id,invoice_id,source_attempt,ordinal,due_at,state,deliveries) VALUES(?,?,?,?,?,'leased',1)", []any{"legacy-job", e.InvoiceID, e.AttemptID, 1, start.UnixMilli()}},
		{"INSERT INTO gateway_receipts VALUES(?,?,?,?,?)", []any{"legacy-job", e.InvoiceID, "provider-attempt", "succeeded", ""}},
	}
	for _, statement := range statements {
		if _, err = db.Exec(statement.q, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	s := openTest(t, path)
	state := snapshot(t, s)
	if state.Events != 1 || state.Attempts != 1 || state.ProviderEffects != 1 || state.ReviewJobs != 1 || state.Pending != 0 {
		t.Fatalf("lost legacy data: %+v", state)
	}
	entries, err := s.Inbox(ctx)
	if err != nil || len(entries) != 1 || entries[0].Reason != "migrated_accepted_event" {
		t.Fatalf("%+v %v", entries, err)
	}
	if result, err := s.Ingest(ctx, e, start); err != nil || !result.DuplicateDelivery {
		t.Fatalf("replay: %+v %v", result, err)
	}
	reopened := openTest(t, path)
	if snapshot(t, reopened).ProviderEffects != 1 {
		t.Fatal("migration not idempotent")
	}
}

func TestMigrationFailureRollsBackSchemaAndData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(legacySchema); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("INSERT INTO events VALUES('bad','not-json',0)"); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(path); err == nil {
		s.Close()
		t.Fatal("migration accepted invalid legacy event")
	}
	var version, count int
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT count(*) FROM events").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if version != 0 || count != 1 {
		t.Fatalf("partial migration: version=%d count=%d", version, count)
	}
	if _, err = db.Exec("SELECT mode FROM jobs"); err == nil {
		t.Fatal("schema changes survived rollback")
	}
	if _, err = db.Exec("SELECT * FROM jobs_legacy"); err == nil {
		t.Fatal("temporary table survived rollback")
	}
}
