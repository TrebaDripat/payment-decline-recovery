package recovery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// v0.1 did not set user_version. Keep its schema here to migrate both a fresh
// database and an existing database in one atomic writer transaction.
const legacySchema = `
 CREATE TABLE IF NOT EXISTS invoices(id TEXT PRIMARY KEY, current_attempt TEXT NOT NULL, state TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY, invoice_id TEXT NOT NULL REFERENCES invoices(id), ordinal INTEGER NOT NULL, status TEXT NOT NULL, code TEXT NOT NULL, UNIQUE(invoice_id,ordinal));
 CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY, payload TEXT NOT NULL, received_at INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, invoice_id TEXT NOT NULL REFERENCES invoices(id), source_attempt TEXT NOT NULL UNIQUE REFERENCES attempts(id), ordinal INTEGER NOT NULL, due_at INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('queued','leased','done')), lease_until INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0, deliveries INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '');
 CREATE INDEX IF NOT EXISTS jobs_due ON jobs(state,due_at,lease_until);
 CREATE TABLE IF NOT EXISTS gateway_receipts(idempotency_key TEXT PRIMARY KEY, invoice_id TEXT NOT NULL, attempt_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL, code TEXT NOT NULL);`

func (s *Store) migrate(ctx context.Context) error {
	return s.write(ctx, func(c *sql.Conn) error {
		var version int
		if err := c.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
			return err
		}
		if version == 2 {
			return nil
		}
		if version != 0 {
			return fmt.Errorf("unsupported database schema version %d", version)
		}
		if _, err := c.ExecContext(ctx, legacySchema); err != nil {
			return err
		}
		_, err := c.ExecContext(ctx, `
   ALTER TABLE jobs RENAME TO jobs_legacy;
   CREATE TABLE jobs(id TEXT PRIMARY KEY, invoice_id TEXT NOT NULL REFERENCES invoices(id), source_attempt TEXT NOT NULL UNIQUE REFERENCES attempts(id), ordinal INTEGER NOT NULL, due_at INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('queued','leased','done','needs_review')), lease_until INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0, deliveries INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '', mode TEXT NOT NULL DEFAULT 'recover' CHECK(mode IN ('recover','reconcile')), resolution_deadline INTEGER NOT NULL DEFAULT 0);
   INSERT INTO jobs(id,invoice_id,source_attempt,ordinal,due_at,state,lease_until,generation,deliveries,last_error,mode)
    SELECT id,invoice_id,source_attempt,ordinal,due_at,
     CASE WHEN state!='done' AND deliveries>0 THEN 'needs_review' ELSE state END,
     lease_until,generation,deliveries,
     CASE WHEN state!='done' AND deliveries>0 THEN 'legacy_delivery_requires_review' ELSE last_error END,
     CASE WHEN deliveries>0 THEN 'reconcile' ELSE 'recover' END FROM jobs_legacy;
   DROP TABLE jobs_legacy;
   CREATE INDEX jobs_due ON jobs(state,due_at,lease_until);
   UPDATE invoices SET state='manual_review' WHERE state!='paid' AND id IN(SELECT invoice_id FROM jobs WHERE state='needs_review');
   CREATE TABLE inbox(event_id TEXT NOT NULL,payload_hash TEXT NOT NULL,payload TEXT NOT NULL,invoice_id TEXT NOT NULL,attempt_id TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('processed','duplicate','needs_review')),reason TEXT NOT NULL,received_at INTEGER NOT NULL,last_received_at INTEGER NOT NULL,deliveries INTEGER NOT NULL DEFAULT 1,PRIMARY KEY(event_id,payload_hash));
   CREATE INDEX inbox_received ON inbox(last_received_at,event_id);
  `)
		if err != nil {
			return err
		}
		// Backfill accepted events without inventing historical delivery counts.
		rows, err := c.QueryContext(ctx, "SELECT payload,received_at FROM events")
		if err != nil {
			return err
		}
		type oldEvent struct {
			payload  string
			received int64
		}
		var events []oldEvent
		for rows.Next() {
			var e oldEvent
			if err = rows.Scan(&e.payload, &e.received); err != nil {
				rows.Close()
				return err
			}
			events = append(events, e)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, old := range events {
			var e Event
			if err = json.Unmarshal([]byte(old.payload), &e); err != nil {
				return fmt.Errorf("invalid legacy event: %w", err)
			}
			if err = e.Validate(); err != nil {
				return fmt.Errorf("invalid legacy event: %w", err)
			}
			canonical, _ := json.Marshal(e)
			if _, err = c.ExecContext(ctx, "INSERT INTO inbox VALUES(?,?,?,?,?,'processed','migrated_accepted_event',?,?,1)", e.EventID, digest(canonical), string(canonical), e.InvoiceID, e.AttemptID, old.received, old.received); err != nil {
				return err
			}
		}
		_, err = c.ExecContext(ctx, "PRAGMA user_version=2")
		return err
	})
}
