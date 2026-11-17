package recovery

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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
	_, err = db.Exec(`PRAGMA journal_mode=WAL;
 CREATE TABLE IF NOT EXISTS invoices(id TEXT PRIMARY KEY, current_attempt TEXT NOT NULL, state TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY, invoice_id TEXT NOT NULL REFERENCES invoices(id), ordinal INTEGER NOT NULL, status TEXT NOT NULL, code TEXT NOT NULL, UNIQUE(invoice_id,ordinal));
 CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY, payload TEXT NOT NULL, received_at INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, invoice_id TEXT NOT NULL REFERENCES invoices(id), source_attempt TEXT NOT NULL UNIQUE REFERENCES attempts(id), ordinal INTEGER NOT NULL, due_at INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('queued','leased','done')), lease_until INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0, deliveries INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '');
 CREATE INDEX IF NOT EXISTS jobs_due ON jobs(state,due_at,lease_until);
 CREATE TABLE IF NOT EXISTS gateway_receipts(idempotency_key TEXT PRIMARY KEY, invoice_id TEXT NOT NULL, attempt_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL, code TEXT NOT NULL);`)
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

// A delivery ID and a business attempt ID are separate deduplication boundaries.
func (s *Store) Ingest(ctx context.Context, e Event, now time.Time) (bool, error) {
	if err := e.Validate(); err != nil {
		return false, err
	}
	payload, _ := json.Marshal(e)
	duplicate := false
	err := s.write(ctx, func(c *sql.Conn) error {
		var previous string
		err := c.QueryRowContext(ctx, "SELECT payload FROM events WHERE id=?", e.EventID).Scan(&previous)
		if err == nil {
			if previous != string(payload) {
				return ErrConflict
			}
			duplicate = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var invoice, status, code string
		err = c.QueryRowContext(ctx, "SELECT invoice_id,status,code FROM attempts WHERE id=?", e.AttemptID).Scan(&invoice, &status, &code)
		if err == nil {
			if invoice != e.InvoiceID || status != e.Status || code != e.Code {
				return ErrConflict
			}
		} else if errors.Is(err, sql.ErrNoRows) {
			var exists int
			if err = c.QueryRowContext(ctx, "SELECT count(*) FROM invoices WHERE id=?", e.InvoiceID).Scan(&exists); err != nil {
				return err
			}
			// This demo owns all retries after the initial terminal event. Unknown later attempts are quarantined.
			if exists != 0 {
				return ErrConflict
			}
			if _, err = c.ExecContext(ctx, "INSERT INTO invoices VALUES(?,?,?)", e.InvoiceID, e.AttemptID, "new"); err != nil {
				return err
			}
			if err = recordOutcome(ctx, c, e.InvoiceID, e.AttemptID, e.Status, e.Code, 0, now); err != nil {
				return err
			}
		} else {
			return err
		}
		_, err = c.ExecContext(ctx, "INSERT INTO events VALUES(?,?,?)", e.EventID, string(payload), now.UnixMilli())
		return err
	})
	return duplicate, err
}

func (s *Store) Claim(ctx context.Context, now time.Time) (*Job, error) {
	var j Job
	err := s.db.QueryRowContext(ctx, `UPDATE jobs SET state='leased',lease_until=?,generation=generation+1,deliveries=deliveries+1
 WHERE id=(SELECT id FROM jobs WHERE (state='queued' AND due_at<=?) OR (state='leased' AND lease_until<=?) ORDER BY due_at,id LIMIT 1)
 RETURNING id,invoice_id,source_attempt,ordinal,generation,deliveries`, now.Add(LeaseDuration).UnixMilli(), now.UnixMilli(), now.UnixMilli()).Scan(&j.ID, &j.InvoiceID, &j.SourceAttempt, &j.Ordinal, &j.Generation, &j.Deliveries)
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
	// Ambiguous provider failures retry the same job/key; they never become declines.
	delay := time.Second * time.Duration(1<<uint(min(j.Deliveries, 8)))
	result, err := s.db.ExecContext(ctx, "UPDATE jobs SET state='queued',due_at=?,last_error='provider result unknown; retry same key' WHERE id=? AND state='leased' AND generation=? AND lease_until>?", now.Add(delay).UnixMilli(), j.ID, j.Generation, now.UnixMilli())
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
	ProviderEffects int       `json:"simulated_provider_effects"`
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
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM jobs WHERE state!='done'").Scan(&out.Pending); err != nil {
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
