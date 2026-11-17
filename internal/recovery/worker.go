package recovery

import (
	"context"
	"errors"
	"time"
)

type Gateway interface {
	Recover(context.Context, Job) (Receipt, error)
	Lookup(context.Context, Job) (Receipt, error)
}

var ErrReceiptNotFound = errors.New("provider authoritatively has no receipt for this key")

type GatewayError struct {
	Kind       string
	RetryAfter time.Duration
}

func (e *GatewayError) Error() string { return "gateway " + e.Kind }

func Step(ctx context.Context, s *Store, g Gateway, now func() time.Time) (bool, error) {
	j, err := s.Claim(ctx, now())
	if err != nil || j == nil {
		return false, err
	}
	if now().UnixMilli() >= j.ResolutionDeadline {
		return true, s.Review(ctx, *j, "resolution_window_elapsed", now())
	}
	var result Receipt
	if j.Mode == "reconcile" {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		result, err = g.Lookup(callCtx, *j)
		cancel()
		if err == nil {
			return true, finishResult(ctx, s, *j, result, now())
		}
		if !errors.Is(err, ErrReceiptNotFound) {
			return true, handleGatewayError(ctx, s, *j, err, "reconcile", now())
		}
	}
	if now().UnixMilli() >= j.ResolutionDeadline {
		return true, s.Review(ctx, *j, "resolution_window_elapsed", now())
	}
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.CheckLease(ctx, *j, now()); err != nil {
		return true, err
	}
	result, err = g.Recover(callCtx, *j)
	cancel()
	if err != nil {
		return true, handleGatewayError(ctx, s, *j, err, "recover", now())
	}
	return true, finishResult(ctx, s, *j, result, now())
}

func finishResult(ctx context.Context, s *Store, j Job, r Receipt, now time.Time) error {
	if err := (Event{EventID: "result", InvoiceID: j.InvoiceID, AttemptID: r.AttemptID, Status: r.Status, Code: r.Code}).Validate(); err != nil {
		return s.Review(ctx, j, "invalid_provider_receipt", now)
	}
	return s.Finish(ctx, j, r, now)
}

func handleGatewayError(ctx context.Context, s *Store, j Job, cause error, operation string, now time.Time) error {
	var providerErr *GatewayError
	if errors.As(cause, &providerErr) && providerErr.Kind == "rejected" {
		return s.Review(ctx, j, "provider_rejected_request", now)
	}
	mode := "reconcile"
	reason := "provider_result_unknown"
	delay := time.Duration(0)
	if errors.As(cause, &providerErr) && providerErr.Kind == "temporary" {
		mode = operation
		reason = "provider_temporarily_unavailable"
		delay = providerErr.RetryAfter
	}
	if err := s.Reschedule(ctx, j, mode, reason, delay, now); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}
