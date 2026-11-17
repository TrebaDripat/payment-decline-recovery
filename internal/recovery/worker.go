package recovery

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type Gateway interface {
	Recover(context.Context, Job) (Receipt, error)
}

// SimulatedGateway is a durable fake, not an adapter to a payment network.
// Its commit happens BEFORE the engine commit, exposing the real crash window.
type SimulatedGateway struct {
	Store   *Store
	Outcome string
}

func (g SimulatedGateway) Recover(ctx context.Context, j Job) (Receipt, error) {
	var r Receipt
	err := g.Store.write(ctx, func(c *sql.Conn) error {
		var invoice string
		err := c.QueryRowContext(ctx, "SELECT invoice_id,attempt_id,status,code FROM gateway_receipts WHERE idempotency_key=?", j.ID).Scan(&invoice, &r.AttemptID, &r.Status, &r.Code)
		if err == nil {
			if invoice != j.InvoiceID {
				return ErrConflict
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		r = Receipt{AttemptID: "attempt-" + j.ID, Status: "succeeded"}
		switch g.Outcome {
		case "success":
		case "soft":
			r.Status = "failed"
			r.Code = "insufficient_funds"
		case "hard":
			r.Status = "failed"
			r.Code = "lost_card"
		default:
			return errors.New("unknown simulated outcome")
		}
		_, err = c.ExecContext(ctx, "INSERT INTO gateway_receipts VALUES(?,?,?,?,?)", j.ID, j.InvoiceID, r.AttemptID, r.Status, r.Code)
		return err
	})
	return r, err
}

func Step(ctx context.Context, s *Store, g Gateway, now func() time.Time) (bool, error) {
	j, err := s.Claim(ctx, now())
	if err != nil || j == nil {
		return false, err
	}
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := g.Recover(callCtx, *j)
	if err != nil {
		if releaseErr := s.Release(ctx, *j, now()); releaseErr != nil {
			return true, errors.Join(err, releaseErr)
		}
		return true, err
	}
	return true, s.Finish(ctx, *j, result, now())
}
