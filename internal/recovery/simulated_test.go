package recovery

import (
	"context"
	"database/sql"
	"errors"
)

// Local fake for fast unit tests; production CLI uses the separate HTTP simulator.
func (g SimulatedGateway) Lookup(ctx context.Context, j Job) (Receipt, error) {
	var r Receipt
	var invoice string
	err := g.Store.db.QueryRowContext(ctx, "SELECT invoice_id,attempt_id,status,code FROM gateway_receipts WHERE idempotency_key=?", j.ID).Scan(&invoice, &r.AttemptID, &r.Status, &r.Code)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrReceiptNotFound
	}
	if err != nil {
		return r, err
	}
	if invoice != j.InvoiceID {
		return r, &GatewayError{Kind: "rejected"}
	}
	return r, nil
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
