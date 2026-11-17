package recovery

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"time"

	_ "modernc.org/sqlite"
)

var ErrConflict = errors.New("conflicting immutable attempt or event")
var ErrStaleLease = errors.New("lease is no longer owned by this worker")
var identifier = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,96}$`)

const MaxRetries = 3
const LeaseDuration = 30 * time.Second
const ResolutionWindow = 15 * time.Minute

type Event struct {
	EventID   string `json:"event_id"`
	InvoiceID string `json:"invoice_id"`
	AttemptID string `json:"attempt_id"`
	Status    string `json:"status"`
	Code      string `json:"code,omitempty"`
}

func (e Event) Validate() error {
	if !identifier.MatchString(e.EventID) || !identifier.MatchString(e.InvoiceID) || !identifier.MatchString(e.AttemptID) {
		return errors.New("IDs must be 1–96 ASCII letters, digits, underscores or hyphens")
	}
	if e.Status == "succeeded" && e.Code == "" {
		return nil
	}
	if e.Status == "failed" && (e.Code == "insufficient_funds" || e.Code == "do_not_honor" || e.Code == "lost_card") {
		return nil
	}
	return errors.New("unsupported terminal outcome or decline code")
}

type Store struct{ db *sql.DB }
type Job struct {
	ID, InvoiceID, SourceAttempt    string
	Ordinal, Generation, Deliveries int
	Mode                            string
	ResolutionDeadline              int64
}
type Receipt struct {
	AttemptID string `json:"attempt_id"`
	Status    string `json:"status"`
	Code      string `json:"code,omitempty"`
}

func Open(path string) (*Store, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: absolute}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(10000)")
	q.Add("_pragma", "foreign_keys(1)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err == nil {
		err = s.migrate(context.Background())
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

// BEGIN IMMEDIATE serializes writers across independent processes, before any read.
func (s *Store) write(ctx context.Context, f func(*sql.Conn) error) error {
	c, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err = c.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	defer c.ExecContext(context.Background(), "ROLLBACK")
	if err = f(c); err != nil {
		return err
	}
	_, err = c.ExecContext(ctx, "COMMIT")
	return err
}

func key(invoice, attempt string) string {
	h := sha256.Sum256([]byte(invoice + "\x00" + attempt))
	return "retry-" + hex.EncodeToString(h[:16])
}
func retryDelay(ordinal int) time.Duration { return time.Duration(1<<uint(ordinal-1)) * time.Minute }
func terminalState(status, code string, ordinal int) string {
	if status == "succeeded" {
		return "paid"
	}
	if code != "insufficient_funds" {
		return "manual_review"
	}
	if ordinal >= MaxRetries {
		return "exhausted"
	}
	return "retry_scheduled"
}

func recordOutcome(ctx context.Context, c *sql.Conn, invoice, attempt, status, code string, ordinal int, now time.Time) error {
	if _, err := c.ExecContext(ctx, "INSERT INTO attempts VALUES(?,?,?,?,?)", attempt, invoice, ordinal, status, code); err != nil {
		return err
	}
	state := terminalState(status, code, ordinal)
	if _, err := c.ExecContext(ctx, "UPDATE invoices SET current_attempt=?,state=? WHERE id=?", attempt, state, invoice); err != nil {
		return err
	}
	if state == "retry_scheduled" {
		_, err := c.ExecContext(ctx, "INSERT INTO jobs(id,invoice_id,source_attempt,ordinal,due_at,state) VALUES(?,?,?,?,?,'queued')", key(invoice, attempt), invoice, attempt, ordinal+1, now.Add(retryDelay(ordinal+1)).UnixMilli())
		return err
	}
	return nil
}

func (s *Store) Claim(ctx context.Context, now time.Time) (*Job, error) {
	var j Job
	err := s.db.QueryRowContext(ctx, `UPDATE jobs SET mode=CASE WHEN state='leased' THEN 'reconcile' ELSE mode END, state='leased',lease_until=?,generation=generation+1,deliveries=deliveries+1,resolution_deadline=CASE WHEN resolution_deadline=0 THEN ? ELSE resolution_deadline END
 WHERE id=(SELECT id FROM jobs WHERE (state='queued' AND due_at<=?) OR (state='leased' AND lease_until<=?) ORDER BY due_at,id LIMIT 1)
 RETURNING id,invoice_id,source_attempt,ordinal,generation,deliveries,mode,resolution_deadline`, now.Add(LeaseDuration).UnixMilli(), now.Add(ResolutionWindow).UnixMilli(), now.UnixMilli(), now.UnixMilli()).Scan(&j.ID, &j.InvoiceID, &j.SourceAttempt, &j.Ordinal, &j.Generation, &j.Deliveries, &j.Mode, &j.ResolutionDeadline)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &j, nil
}

func (s *Store) Finish(ctx context.Context, j Job, r Receipt, now time.Time) error {
	if !identifier.MatchString(r.AttemptID) {
		return errors.New("invalid provider attempt ID")
	}
	if err := (Event{EventID: "result", InvoiceID: j.InvoiceID, AttemptID: r.AttemptID, Status: r.Status, Code: r.Code}).Validate(); err != nil {
		return err
	}
	return s.write(ctx, func(c *sql.Conn) error {
		result, err := c.ExecContext(ctx, "UPDATE jobs SET state='done',last_error='' WHERE id=? AND state='leased' AND generation=? AND lease_until>?", j.ID, j.Generation, now.UnixMilli())
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrStaleLease
		}
		return recordOutcome(ctx, c, j.InvoiceID, r.AttemptID, r.Status, r.Code, j.Ordinal, now)
	})
}

func (s *Store) Release(ctx context.Context, j Job, now time.Time) error {
	return s.Reschedule(ctx, j, "reconcile", "provider_result_unknown", 0, now)
}

func (s *Store) CheckLease(ctx context.Context, j Job, now time.Time) error {
	var valid int
	err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM jobs WHERE id=? AND generation=? AND state='leased' AND lease_until>?", j.ID, j.Generation, now.UnixMilli()).Scan(&valid)
	if err != nil {
		return err
	}
	if valid != 1 {
		return ErrStaleLease
	}
	return nil
}

func (s *Store) Reschedule(ctx context.Context, j Job, mode, reason string, retryAfter time.Duration, now time.Time) error {
	if mode != "recover" && mode != "reconcile" {
		return errors.New("invalid job mode")
	}
	delay := max(time.Second*time.Duration(1<<uint(min(j.Deliveries, 8))), retryAfter)
	due := min(now.Add(delay).UnixMilli(), j.ResolutionDeadline)
	result, err := s.db.ExecContext(ctx, "UPDATE jobs SET state='queued',due_at=?,mode=?,last_error=? WHERE id=? AND state='leased' AND generation=? AND lease_until>?", due, mode, reason, j.ID, j.Generation, now.UnixMilli())
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrStaleLease
	}
	return nil
}

func (s *Store) Review(ctx context.Context, j Job, reason string, now time.Time) error {
	return s.write(ctx, func(c *sql.Conn) error {
		r, err := c.ExecContext(ctx, "UPDATE jobs SET state='needs_review',last_error=? WHERE id=? AND state='leased' AND generation=? AND lease_until>?", reason, j.ID, j.Generation, now.UnixMilli())
		if err != nil {
			return err
		}
		n, err := r.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrStaleLease
		}
		_, err = c.ExecContext(ctx, "UPDATE invoices SET state='manual_review' WHERE id=? AND state!='paid'", j.InvoiceID)
		return err
	})
}

type Invoice struct {
	ID             string `json:"id"`
	State          string `json:"state"`
	CurrentAttempt string `json:"current_attempt"`
}
type Snapshot struct {
	Events          int       `json:"webhook_events"`
	Attempts        int       `json:"payment_attempts"`
	Jobs            int       `json:"retry_jobs"`
	Pending         int       `json:"pending_jobs"`
	ProviderEffects int       `json:"legacy_provider_receipts"`
	ReviewJobs      int       `json:"review_jobs"`
	InboxReview     int       `json:"inbox_needs_review"`
	Invoices        []Invoice `json:"invoices"`
}

func (s *Store) Snapshot(ctx context.Context) (Snapshot, error) {
	out := Snapshot{Invoices: []Invoice{}}
	// A read transaction makes the counters and invoice states one consistent snapshot.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	for _, q := range []struct {
		table  string
		target *int
	}{{"events", &out.Events}, {"attempts", &out.Attempts}, {"jobs", &out.Jobs}, {"gateway_receipts", &out.ProviderEffects}} {
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM "+q.table).Scan(q.target); err != nil {
			return out, err
		}
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM jobs WHERE state IN ('queued','leased')").Scan(&out.Pending); err != nil {
		return out, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM jobs WHERE state='needs_review'").Scan(&out.ReviewJobs); err != nil {
		return out, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM inbox WHERE status='needs_review'").Scan(&out.InboxReview); err != nil {
		return out, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT id,state,current_attempt FROM invoices ORDER BY id")
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var i Invoice
		if err = rows.Scan(&i.ID, &i.State, &i.CurrentAttempt); err != nil {
			rows.Close()
			return out, err
		}
		out.Invoices = append(out.Invoices, i)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }
func (j Job) String() string                    { return fmt.Sprintf("%s generation=%d", j.ID, j.Generation) }
