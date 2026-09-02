package services

import (
	"context"

	"github.com/YMX899/Yuex/examples/backend/persistence"
)

// Reserve runs before submit so concurrent requests cannot all spend the same
// credit balance. The amount is a product estimate, not a Runtime decision.
func (s UsageSettlement) Reserve(ctx context.Context, command persistence.ReservationCommand) (string, error) {
	return s.Usage.ReserveOnce(ctx, command)
}

// SettleTerminal stores Runtime metering once, then settles or releases the
// reservation once. Unique keys must make reprocessing harmless.
func (s UsageSettlement) SettleTerminal(ctx context.Context, runID string) error {
	run, err := s.Runs.Get(ctx, runID)
	if err != nil {
		return err
	}
	usage, err := s.Runtime.GetRawUsage(ctx, runID)
	if err != nil {
		return err
	}
	recordID, err := s.Usage.RecordRawUsageOnce(ctx, "runtime-usage:"+runID, usage)
	if err != nil {
		return err
	}
	if run.Status == "succeeded" {
		return s.Usage.SettleReservationOnce(ctx, runID, recordID)
	}
	return s.Usage.ReleaseReservationOnce(ctx, runID, run.Status)
}

type UsageSettlement struct {
	Runs    persistence.RunRepository
	Usage   persistence.UsageRepository
	Runtime persistence.RawUsageReader
}
