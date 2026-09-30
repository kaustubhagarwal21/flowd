package pgstore

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// SetIdleTxTimeout shortens idleTxTimeout for one Store, so a test does not
// wait the full production value.
func SetIdleTxTimeout(s *Store, d time.Duration) { s.setIdleTxTimeout(d) }

// HoldRunLock plays a flowd process that freezes in the middle of a
// transaction: it locks the run the way CompleteStep does, closes locked,
// and sends nothing more until release is closed. It returns the error of
// the commit that follows.
func HoldRunLock(ctx context.Context, s *Store, runID string, locked chan<- struct{}, release <-chan struct{}) error {
	return pgx.BeginTxFunc(ctx, s.pool, s.writeTx, func(tx pgx.Tx) error {
		if _, err := lockRun(ctx, tx, runID); err != nil {
			return err
		}
		close(locked)
		<-release
		return nil
	})
}
