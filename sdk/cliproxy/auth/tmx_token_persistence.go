package auth

import (
	"context"
	"errors"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
)

type tmxTokenPersistenceKey struct{}

// WithTmxTokenPersistence requires a management Google quota refresh to save
// its tokens before publication. Ordinary Modify and Meta mint keep their rules.
func WithTmxTokenPersistence(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, tmxTokenPersistenceKey{}, true)
}

func tmxRequiresTokenPersistence(ctx context.Context, a *Auth) bool {
	if ctx == nil {
		return false
	}
	required, _ := ctx.Value(tmxTokenPersistenceKey{}).(bool)
	return required && a != nil && refusals.GoogleProvider(a.Provider)
}

var errTmxTokenPersistence = errors.New("account token update could not be persisted")
