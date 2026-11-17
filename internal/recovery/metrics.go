package recovery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Metrics reads durable aggregates in one snapshot, including work performed by
// other processes. No invoice, event, attempt or job identifiers become labels.
func (s *Store) Metrics(ctx context.Context, now time.Time) (string, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	jobs, err := metricGroups(ctx, tx, "SELECT state || '/' || mode, count(*) FROM jobs GROUP BY state,mode")
	if err != nil {
		return "", err
	}
	var claims, ready, oldest int64
	if err = tx.QueryRowContext(ctx, "SELECT coalesce(sum(deliveries),0) FROM jobs").Scan(&claims); err != nil {
		return "", err
	}
	// A future scheduled retry is not queue lag. Expired leases become eligible
	// at lease_until, rather than at their original business retry due time.
	if err = tx.QueryRowContext(ctx, `SELECT count(*), coalesce(min(eligible_at),?) FROM (
	 SELECT CASE WHEN state='queued' THEN due_at ELSE lease_until END AS eligible_at
	 FROM jobs WHERE (state='queued' AND due_at<=?) OR (state='leased' AND lease_until<=?)
	)`, now.UnixMilli(), now.UnixMilli(), now.UnixMilli()).Scan(&ready, &oldest); err != nil {
		return "", err
	}
	deliveries, err := metricGroups(ctx, tx, "SELECT status,sum(deliveries) FROM inbox GROUP BY status")
	if err != nil {
		return "", err
	}
	var redeliveries int64
	if err = tx.QueryRowContext(ctx, "SELECT coalesce(sum(deliveries-1),0) FROM inbox").Scan(&redeliveries); err != nil {
		return "", err
	}
	attempts, err := metricGroups(ctx, tx, "SELECT status,count(*) FROM attempts GROUP BY status")
	if err != nil {
		return "", err
	}
	invoices, err := metricGroups(ctx, tx, "SELECT state,count(*) FROM invoices GROUP BY state")
	if err != nil {
		return "", err
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	var out strings.Builder
	family := func(name, kind, help string) {
		fmt.Fprintf(&out, "# HELP recovery_%s %s\n# TYPE recovery_%s %s\n", name, help, name, kind)
	}
	family("jobs", "gauge", "Current jobs by state and operation mode.")
	for _, state := range []string{"queued", "leased", "done", "needs_review"} {
		for _, mode := range []string{"recover", "reconcile"} {
			fmt.Fprintf(&out, "recovery_jobs{state=%q,mode=%q} %d\n", state, mode, jobs[state+"/"+mode])
		}
	}
	family("job_claims_total", "counter", "Persisted job claims, including lease reclamations; not provider requests.")
	fmt.Fprintf(&out, "recovery_job_claims_total %d\n", claims)
	family("ready_jobs", "gauge", "Jobs eligible for claim now, including expired leases.")
	fmt.Fprintf(&out, "recovery_ready_jobs %d\n", ready)
	family("oldest_ready_job_age_seconds", "gauge", "Seconds since the oldest eligible job became claimable; zero when none.")
	fmt.Fprintf(&out, "recovery_oldest_ready_job_age_seconds %.3f\n", float64(max(0, now.UnixMilli()-oldest))/1000)
	family("webhook_deliveries_total", "counter", "Journaled valid deliveries grouped by their payload's original inbox decision.")
	for _, decision := range []string{"processed", "duplicate", "needs_review"} {
		fmt.Fprintf(&out, "recovery_webhook_deliveries_total{decision=%q} %d\n", decision, deliveries[decision])
	}
	family("webhook_redeliveries_total", "counter", "Repeated identical canonical payloads under the same event ID.")
	fmt.Fprintf(&out, "recovery_webhook_redeliveries_total %d\n", redeliveries)
	family("payment_attempts_total", "counter", "Recorded terminal attempts, including initial webhook attempts; not provider effects.")
	for _, status := range []string{"failed", "succeeded"} {
		fmt.Fprintf(&out, "recovery_payment_attempts_total{status=%q} %d\n", status, attempts[status])
	}
	family("invoices", "gauge", "Current invoices by recovery state.")
	for _, state := range []string{"paid", "retry_scheduled", "manual_review", "exhausted"} {
		fmt.Fprintf(&out, "recovery_invoices{state=%q} %d\n", state, invoices[state])
	}
	return out.String(), nil
}

func metricGroups(ctx context.Context, tx *sql.Tx, query string) (map[string]int64, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var label string
		var count int64
		if err = rows.Scan(&label, &count); err != nil {
			return nil, err
		}
		out[label] = count
	}
	return out, rows.Err()
}
