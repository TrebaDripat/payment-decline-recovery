package recovery

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

const testSecret = "disposable-test-secret-not-a-credential"

func TestWebhookHTTPContract(t *testing.T) {
	cases := []struct {
		name         string
		body         string
		timestamp    time.Time
		badSignature bool
		want         int
	}{
		{"valid", `{"event_id":"e","invoice_id":"i","attempt_id":"a","status":"failed","code":"insufficient_funds"}`, start, false, 200},
		{"tampered", `{"event_id":"e"}`, start, true, 401},
		{"expired", `{}`, start.Add(-301 * time.Second), false, 401},
		{"future", `{}`, start.Add(301 * time.Second), false, 401},
		{"bad JSON", `{`, start, false, 400},
		{"unknown field", `{"surprise":true}`, start, false, 400},
		{"extra JSON", `{"event_id":"e","invoice_id":"i","attempt_id":"a","status":"succeeded"} {}`, start, false, 400},
		{"invalid status", `{"event_id":"e","invoice_id":"i","attempt_id":"a","status":"pending"}`, start, false, 400},
		{"unknown code", `{"event_id":"e","invoice_id":"i","attempt_id":"a","status":"failed","code":"provider_outage"}`, start, false, 400},
		{"oversized", strings.Repeat(" ", 65537), start, false, 413},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := testStore(t)
			h, err := Handler(s, testSecret, func() time.Time { return start })
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPost, "/webhooks", strings.NewReader(tc.body))
			ts := strconv.FormatInt(tc.timestamp.Unix(), 10)
			r.Header.Set("X-Webhook-Timestamp", ts)
			sig := Signature(testSecret, ts, []byte(tc.body))
			if tc.badSignature {
				sig = "deadbeef"
			}
			r.Header.Set("X-Webhook-Signature", sig)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("got %d: %s", w.Code, w.Body.String())
			}
			if tc.want != 200 && snapshot(t, s).Events != 0 {
				t.Fatal("rejected request changed database")
			}
		})
	}
}
func TestHTTPRedeliveryAndConflict(t *testing.T) {
	s := testStore(t)
	h, err := Handler(s, testSecret, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	e := event("a")
	send := func(e Event) *httptest.ResponseRecorder {
		body, _ := json.Marshal(e)
		r := httptest.NewRequest("POST", "/webhooks", bytes.NewReader(body))
		ts := strconv.FormatInt(start.Unix(), 10)
		r.Header.Set("X-Webhook-Timestamp", ts)
		r.Header.Set("X-Webhook-Signature", Signature(testSecret, ts, body))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := send(e); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if w := send(e); w.Code != 200 || !strings.Contains(w.Body.String(), `"duplicate_delivery":true`) {
		t.Fatal(w.Body.String())
	}
	e.Code = "lost_card"
	if w := send(e); w.Code != 202 || !strings.Contains(w.Body.String(), `"status":"needs_review"`) {
		t.Fatalf("got %d", w.Code)
	}
	entries, err := s.Inbox(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || snapshot(t, s).InboxReview != 1 {
		t.Fatalf("missing persisted conflict: %+v", entries)
	}
}
