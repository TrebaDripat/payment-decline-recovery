package recovery

import (
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMetricsFollowDurableCrossConnectionWork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.db")
	api := openTest(t, path)
	worker := openTest(t, path)
	e := event("private-invoice")
	ingest(t, api, e)
	ingest(t, api, e)
	e.EventID = "alias"
	ingest(t, api, e)
	now := start.Add(time.Minute)
	job := claim(t, worker, now)
	if err := worker.Release(ctx, *job, now); err != nil {
		t.Fatal(err)
	}
	check := func(store *Store, when time.Time, samples ...string) {
		t.Helper()
		body, err := store.Metrics(ctx, when)
		if err != nil {
			t.Fatal(err)
		}
		for _, sample := range samples {
			if !strings.Contains("\n"+body, "\n"+sample+"\n") {
				t.Fatalf("missing sample %q in\n%s", sample, body)
			}
		}
		if strings.Contains(body, "private-invoice") || strings.Contains(body, job.ID) {
			t.Fatal("identifiers leaked into metrics")
		}
	}
	check(api, now, "recovery_ready_jobs 0", "recovery_oldest_ready_job_age_seconds 0.000")
	now = now.Add(5 * time.Second)
	check(api, now,
		`recovery_jobs{state="queued",mode="reconcile"} 1`,
		`recovery_webhook_deliveries_total{decision="processed"} 2`,
		`recovery_webhook_deliveries_total{decision="duplicate"} 1`,
		"recovery_webhook_redeliveries_total 1", "recovery_job_claims_total 1",
		"recovery_ready_jobs 1", "recovery_oldest_ready_job_age_seconds 3.000")
	job = claim(t, worker, now)
	check(api, now.Add(LeaseDuration+time.Second), "recovery_ready_jobs 1", "recovery_oldest_ready_job_age_seconds 1.000")
	if err := worker.Finish(ctx, *job, Receipt{AttemptID: "recovered", Status: "succeeded"}, now); err != nil {
		t.Fatal(err)
	}
	worker.Close()
	api.Close()
	check(openTest(t, path), now,
		`recovery_jobs{state="done",mode="reconcile"} 1`,
		`recovery_invoices{state="paid"} 1`,
		`recovery_payment_attempts_total{status="failed"} 1`,
		`recovery_payment_attempts_total{status="succeeded"} 1`,
		"recovery_job_claims_total 2", "recovery_ready_jobs 0", "recovery_oldest_ready_job_age_seconds 0.000")
}

func TestMetricsHTTPFailureDoesNotReportHealthyZeroes(t *testing.T) {
	s := testStore(t)
	h, err := Handler(s, testSecret, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 || w.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" || !strings.Contains(w.Body.String(), "recovery_ready_jobs 0\n") {
		t.Fatalf("unexpected response: %d %s", w.Code, w.Body.String())
	}
	s.Close()
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 503 || strings.Contains(w.Body.String(), "recovery_ready_jobs") {
		t.Fatalf("failed scrape must not look healthy: %d %s", w.Code, w.Body.String())
	}
}
