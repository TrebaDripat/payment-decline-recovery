package recovery

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/TrebaDripat/payment-decline-recovery/internal/apidocs"
)

func Signature(secret, timestamp string, body []byte) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(timestamp + "."))
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

func Handler(s *Store, secret string, now func() time.Time) (http.Handler, error) {
	if len(secret) < 16 {
		return nil, errors.New("webhook secret must contain at least 16 bytes")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /openapi.json", apidocs.Engine)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		metrics, err := s.Metrics(ctx, now())
		if err != nil {
			http.Error(w, "metrics storage unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = io.WriteString(w, metrics)
	})
	mux.HandleFunc("POST /webhooks", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
		if err != nil {
			http.Error(w, "body too large or unreadable", http.StatusRequestEntityTooLarge)
			return
		}
		ts := r.Header.Get("X-Webhook-Timestamp")
		seconds, err := strconv.ParseInt(ts, 10, 64)
		instant := now().Unix()
		if err != nil || seconds < instant-300 || seconds > instant+300 {
			http.Error(w, "invalid signature timestamp", http.StatusUnauthorized)
			return
		}
		provided, err := hex.DecodeString(r.Header.Get("X-Webhook-Signature"))
		expected, _ := hex.DecodeString(Signature(secret, ts, body))
		if err != nil || !hmac.Equal(provided, expected) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		var event Event
		// Decode the exact authenticated bytes; reject unknown fields and extra JSON values.
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err = dec.Decode(&event); err != nil {
			http.Error(w, "invalid event JSON", http.StatusBadRequest)
			return
		}
		var extra any
		if dec.Decode(&extra) != io.EOF {
			http.Error(w, "expected one JSON value", http.StatusBadRequest)
			return
		}
		if err = event.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		result, err := s.Ingest(r.Context(), event, now())
		if err != nil {
			http.Error(w, "storage temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if result.Status == "needs_review" {
			w.WriteHeader(http.StatusAccepted)
		}
		writeJSON(w, result)
	})
	mux.HandleFunc("GET /inbox", func(w http.ResponseWriter, r *http.Request) {
		entries, err := s.Inbox(r.Context())
		if err != nil {
			http.Error(w, "storage unavailable", 503)
			return
		}
		writeJSON(w, entries)
	})
	mux.HandleFunc("GET /reviews", func(w http.ResponseWriter, r *http.Request) {
		entries, err := s.Reviews(r.Context())
		if err != nil {
			http.Error(w, "storage unavailable", 503)
			return
		}
		writeJSON(w, entries)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if s.Ping(r.Context()) != nil {
			http.Error(w, "storage unavailable", 503)
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /snapshot", func(w http.ResponseWriter, r *http.Request) {
		state, err := s.Snapshot(r.Context())
		if err != nil {
			http.Error(w, "storage unavailable", 503)
			return
		}
		writeJSON(w, state)
	})
	return mux, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
