// Package simulator provides a durable synthetic gateway in its own database.
package simulator

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"github.com/TrebaDripat/payment-decline-recovery/internal/recovery"
	_ "modernc.org/sqlite"
)

type Store struct{ db *sql.DB }
type Options struct {
	Outcome string
	Fault   string
	Delay   time.Duration
}

func (o Options) Validate() error {
	if o.Outcome != "success" && o.Outcome != "soft" && o.Outcome != "hard" {
		return errors.New("outcome must be success, soft or hard")
	}
	switch o.Fault {
	case "none", "drop_after_commit", "timeout_after_commit", "unavailable", "rate_limit", "reject":
	default:
		return errors.New("unknown simulator fault")
	}
	return nil
}
func Open(path string) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: abs}
	q := u.Query()
	q.Add("_pragma", "busy_timeout(10000)")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA journal_mode=WAL;
 CREATE TABLE IF NOT EXISTS receipts(idempotency_key TEXT PRIMARY KEY,invoice_id TEXT NOT NULL,attempt_id TEXT NOT NULL UNIQUE,status TEXT NOT NULL,code TEXT NOT NULL);`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db}, nil
}
func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Count(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM receipts").Scan(&n)
	return n, err
}
func (s *Store) lookup(ctx context.Context, request recovery.RecoveryRequest) (recovery.Receipt, error) {
	var r recovery.Receipt
	var invoice string
	err := s.db.QueryRowContext(ctx, "SELECT invoice_id,attempt_id,status,code FROM receipts WHERE idempotency_key=?", request.IdempotencyKey).Scan(&invoice, &r.AttemptID, &r.Status, &r.Code)
	if errors.Is(err, sql.ErrNoRows) {
		return r, recovery.ErrReceiptNotFound
	}
	if err != nil {
		return r, err
	}
	if invoice != request.InvoiceID {
		return r, recovery.ErrConflict
	}
	return r, nil
}
func (s *Store) recover(ctx context.Context, request recovery.RecoveryRequest, outcome string) (recovery.Receipt, bool, error) {
	hash := sha256.Sum256([]byte(request.IdempotencyKey))
	r := recovery.Receipt{AttemptID: "attempt-" + hex.EncodeToString(hash[:]), Status: "succeeded"}
	if outcome == "soft" {
		r.Status = "failed"
		r.Code = "insufficient_funds"
	}
	if outcome == "hard" {
		r.Status = "failed"
		r.Code = "lost_card"
	}
	result, err := s.db.ExecContext(ctx, "INSERT INTO receipts VALUES(?,?,?,?,?) ON CONFLICT(idempotency_key) DO NOTHING", request.IdempotencyKey, request.InvoiceID, r.AttemptID, r.Status, r.Code)
	if err != nil {
		return r, false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return r, false, err
	}
	r, err = s.lookup(ctx, request)
	return r, n == 1, err
}

func Handler(s *Store, options Options) (http.Handler, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	if options.Delay <= 0 {
		options.Delay = 10 * time.Second
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /recoveries", func(w http.ResponseWriter, r *http.Request) {
		var request recovery.RecoveryRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&request) != nil || request.Validate() != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			http.Error(w, "expected one JSON value", 400)
			return
		}
		switch options.Fault {
		case "unavailable":
			http.Error(w, "injected unavailable", 503)
			return
		case "rate_limit":
			w.Header().Set("Retry-After", "2")
			http.Error(w, "injected rate limit", 429)
			return
		case "reject":
			http.Error(w, "injected permanent rejection", 422)
			return
		}
		receipt, fresh, err := s.recover(r.Context(), request, options.Outcome)
		if errors.Is(err, recovery.ErrConflict) {
			http.Error(w, "key reused for another invoice", 409)
			return
		}
		if err != nil {
			http.Error(w, "provider storage unavailable", 503)
			return
		}
		if fresh && options.Fault == "drop_after_commit" {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, "connection drop unavailable", 500)
				return
			}
			conn, _, err := hijacker.Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		if fresh && options.Fault == "timeout_after_commit" {
			timer := time.NewTimer(options.Delay)
			defer timer.Stop()
			select {
			case <-r.Context().Done():
				return
			case <-timer.C:
			}
		}
		writeJSON(w, receipt)
	})
	mux.HandleFunc("GET /recoveries/{key}", func(w http.ResponseWriter, r *http.Request) {
		request := recovery.RecoveryRequest{IdempotencyKey: r.PathValue("key"), InvoiceID: r.URL.Query().Get("invoice_id")}
		if request.Validate() != nil {
			http.Error(w, "invalid identity", 400)
			return
		}
		receipt, err := s.lookup(r.Context(), request)
		if errors.Is(err, recovery.ErrReceiptNotFound) {
			http.Error(w, "not found", 404)
			return
		}
		if errors.Is(err, recovery.ErrConflict) {
			http.Error(w, "identity conflict", 409)
			return
		}
		if err != nil {
			http.Error(w, "provider storage unavailable", 503)
			return
		}
		writeJSON(w, receipt)
	})
	mux.HandleFunc("GET /snapshot", func(w http.ResponseWriter, r *http.Request) {
		n, err := s.Count(r.Context())
		if err != nil {
			http.Error(w, "provider storage unavailable", 503)
			return
		}
		writeJSON(w, map[string]int{"provider_effects": n})
	})
	return mux, nil
}
func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
