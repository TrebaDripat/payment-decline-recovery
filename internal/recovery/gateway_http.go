package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type RecoveryRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
	InvoiceID      string `json:"invoice_id"`
}

func (r RecoveryRequest) Validate() error {
	if !identifier.MatchString(r.IdempotencyKey) || !identifier.MatchString(r.InvoiceID) {
		return errors.New("invalid recovery identity")
	}
	return nil
}

// HTTPGateway implements the local simulator protocol, not a card-network API.
type HTTPGateway struct {
	baseURL string
	client  *http.Client
}

func NewHTTPGateway(base string) (*HTTPGateway, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("gateway must be an HTTP(S) URL without credentials, query or fragment")
	}
	return &HTTPGateway{baseURL: strings.TrimRight(base, "/"), client: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (g *HTTPGateway) Recover(ctx context.Context, j Job) (Receipt, error) {
	body, _ := json.Marshal(RecoveryRequest{IdempotencyKey: j.ID, InvoiceID: j.InvoiceID})
	return g.call(ctx, http.MethodPost, g.baseURL+"/recoveries", body)
}
func (g *HTTPGateway) Lookup(ctx context.Context, j Job) (Receipt, error) {
	return g.call(ctx, http.MethodGet, g.baseURL+"/recoveries/"+url.PathEscape(j.ID)+"?invoice_id="+url.QueryEscape(j.InvoiceID), nil)
}
func (g *HTTPGateway) call(ctx context.Context, method, endpoint string, body []byte) (Receipt, error) {
	var out Receipt
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := g.client.Do(request)
	if err != nil {
		return out, &GatewayError{Kind: "unknown"}
	}
	defer response.Body.Close()
	if method == http.MethodGet && response.StatusCode == http.StatusNotFound {
		return out, ErrReceiptNotFound
	}
	if response.StatusCode == http.StatusTooManyRequests {
		// The simulator contract guarantees 429 occurs before accepting an operation.
		delay := time.Second
		value := response.Header.Get("Retry-After")
		if n, err := strconv.ParseInt(value, 10, 64); err == nil && n >= 0 {
			delay = time.Duration(min(n, int64(ResolutionWindow/time.Second))) * time.Second
		} else if date, err := http.ParseTime(value); err == nil {
			delay = min(max(time.Until(date), 0), ResolutionWindow)
		}
		return out, &GatewayError{Kind: "temporary", RetryAfter: delay}
	}
	if response.StatusCode >= 400 && response.StatusCode < 500 {
		return out, &GatewayError{Kind: "rejected"}
	}
	if response.StatusCode != http.StatusOK {
		return out, &GatewayError{Kind: "unknown"}
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return out, &GatewayError{Kind: "unknown"}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&out); err != nil {
		return out, &GatewayError{Kind: "unknown"}
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return out, &GatewayError{Kind: "unknown"}
	}
	if out.AttemptID == "" {
		return out, fmt.Errorf("provider returned an empty attempt identity")
	}
	return out, nil
}
