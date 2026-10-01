package agent

import (
	"context"
	"errors"
)

// ErrSupplementAuthority identifies a confirmed pre-write authority denial.
// Transport errors after a write must not be classified as authority denial.
var ErrSupplementAuthority = errors.New("supplement authority denied")

type supplementAuthorityKey struct{}

// WithSupplementAuthority carries an execution-time coordinator authority gate.
// Provider adapters retain this context until the actual outbound input write.
func WithSupplementAuthority(ctx context.Context, check func(context.Context) error) context.Context {
	return context.WithValue(ctx, supplementAuthorityKey{}, check)
}

// CheckSupplementAuthority leaves ordinary human input unchanged. Coordinator
// gates must verify the durable delivery reservation and current authority.
func CheckSupplementAuthority(ctx context.Context) error {
	check, coordinated := ctx.Value(supplementAuthorityKey{}).(func(context.Context) error)
	if err := ctx.Err(); err != nil {
		if coordinated {
			return errors.Join(ErrSupplementAuthority, err)
		}
		return err
	}
	if coordinated && check != nil {
		if err := check(ctx); err != nil {
			return errors.Join(ErrSupplementAuthority, err)
		}
	}
	return nil
}
