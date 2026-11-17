package recovery

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

type IngestResult struct {
	Status            string `json:"status"`
	DuplicateDelivery bool   `json:"duplicate_delivery"`
	Reason            string `json:"reason,omitempty"`
}

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// Ingest atomically journals a validated event and applies it, or commits a
// needs_review record. The first observed payload permanently reserves its ID.
func (s *Store) Ingest(ctx context.Context, e Event, now time.Time) (IngestResult, error) {
	result := IngestResult{Status: "processed"}
	if err := e.Validate(); err != nil {
		return result, err
	}
	payload, _ := json.Marshal(e)
	hash := digest(payload)
	err := s.write(ctx, func(c *sql.Conn) error {
		err := c.QueryRowContext(ctx, "SELECT status,reason FROM inbox WHERE event_id=? AND payload_hash=?", e.EventID, hash).Scan(&result.Status, &result.Reason)
		if err == nil {
			result.DuplicateDelivery = true
			_, err = c.ExecContext(ctx, "UPDATE inbox SET deliveries=deliveries+1,last_received_at=? WHERE event_id=? AND payload_hash=?", now.UnixMilli(), e.EventID, hash)
			return err
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var count int
		if err = c.QueryRowContext(ctx, "SELECT count(*) FROM inbox WHERE event_id=?", e.EventID).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			result.Status = "needs_review"
			result.Reason = "event_id_reused"
		} else {
			var invoice, status, code string
			err = c.QueryRowContext(ctx, "SELECT invoice_id,status,code FROM attempts WHERE id=?", e.AttemptID).Scan(&invoice, &status, &code)
			if err == nil {
				if invoice != e.InvoiceID || status != e.Status || code != e.Code {
					result.Status = "needs_review"
					result.Reason = "attempt_conflict"
				} else {
					result.Status = "duplicate"
					result.Reason = "attempt_already_recorded"
				}
			} else if errors.Is(err, sql.ErrNoRows) {
				if err = c.QueryRowContext(ctx, "SELECT count(*) FROM invoices WHERE id=?", e.InvoiceID).Scan(&count); err != nil {
					return err
				}
				if count > 0 {
					result.Status = "needs_review"
					result.Reason = "unknown_attempt_for_invoice"
				} else {
					if _, err = c.ExecContext(ctx, "INSERT INTO invoices VALUES(?,?,?)", e.InvoiceID, e.AttemptID, "new"); err != nil {
						return err
					}
					if err = recordOutcome(ctx, c, e.InvoiceID, e.AttemptID, e.Status, e.Code, 0, now); err != nil {
						return err
					}
				}
			} else {
				return err
			}
		}
		if result.Status == "needs_review" {
			// Fence future engine work for an affected invoice; an already dispatched
			// external request cannot be cancelled by this database transaction.
			if _, err = c.ExecContext(ctx, "UPDATE jobs SET state='needs_review',last_error='webhook_conflict' WHERE invoice_id=? AND state IN ('queued','leased')", e.InvoiceID); err != nil {
				return err
			}
			if _, err = c.ExecContext(ctx, "UPDATE invoices SET state='manual_review' WHERE id=? AND state!='paid'", e.InvoiceID); err != nil {
				return err
			}
		} else {
			if _, err = c.ExecContext(ctx, "INSERT INTO events VALUES(?,?,?)", e.EventID, string(payload), now.UnixMilli()); err != nil {
				return err
			}
		}
		_, err = c.ExecContext(ctx, "INSERT INTO inbox VALUES(?,?,?,?,?,?,?,?,?,1)", e.EventID, hash, string(payload), e.InvoiceID, e.AttemptID, result.Status, result.Reason, now.UnixMilli(), now.UnixMilli())
		return err
	})
	return result, err
}

type InboxEntry struct {
	EventID       string          `json:"event_id"`
	PayloadHash   string          `json:"payload_hash"`
	Payload       json.RawMessage `json:"payload"`
	Status        string          `json:"status"`
	Reason        string          `json:"reason"`
	Deliveries    int             `json:"deliveries"`
	FirstReceived int64           `json:"first_received_unix_ms"`
	LastReceived  int64           `json:"last_received_unix_ms"`
}

// Inbox returns a bounded diagnostic page; it is not a full export.
func (s *Store) Inbox(ctx context.Context) ([]InboxEntry, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT event_id,payload_hash,payload,status,reason,deliveries,received_at,last_received_at FROM inbox ORDER BY last_received_at DESC,event_id,payload_hash LIMIT 100")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []InboxEntry{}
	for rows.Next() {
		var e InboxEntry
		var payload string
		if err = rows.Scan(&e.EventID, &e.PayloadHash, &payload, &e.Status, &e.Reason, &e.Deliveries, &e.FirstReceived, &e.LastReceived); err != nil {
			return nil, err
		}
		e.Payload = json.RawMessage(payload)
		out = append(out, e)
	}
	return out, rows.Err()
}

type ReviewJob struct {
	ID         string `json:"id"`
	InvoiceID  string `json:"invoice_id"`
	Reason     string `json:"reason"`
	Mode       string `json:"mode"`
	Deliveries int    `json:"deliveries"`
}

func (s *Store) Reviews(ctx context.Context) ([]ReviewJob, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT id,invoice_id,last_error,mode,deliveries FROM jobs WHERE state='needs_review' ORDER BY id LIMIT 100")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ReviewJob{}
	for rows.Next() {
		var j ReviewJob
		if err = rows.Scan(&j.ID, &j.InvoiceID, &j.Reason, &j.Mode, &j.Deliveries); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
