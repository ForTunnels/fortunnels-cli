// SPDX-License-Identifier: PROPRIETARY
// Copyright (c) 2026 ForTunnels

package control

import "time"

// TrafficQuotaError is terminal for the current CLI invocation. A new
// invocation may proceed after the allowance resets or the plan changes.
type TrafficQuotaError struct {
	ResetAt time.Time
}

func (e *TrafficQuotaError) Error() string { return MonthlyTrafficLimitMessage(e.ResetAt) }

func MonthlyTrafficLimitMessage(resetAt time.Time) string {
	if resetAt.IsZero() {
		return "Monthly traffic limit reached (incoming + outgoing). Change your plan. Reconnecting or creating another tunnel will not reset this account's allowance."
	}
	return "Monthly traffic limit reached (incoming + outgoing). Wait until " +
		resetAt.UTC().Format(time.RFC3339) +
		" or change your plan. Reconnecting or creating another tunnel will not reset this account's allowance."
}
