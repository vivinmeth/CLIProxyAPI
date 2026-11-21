package quota

import (
	"context"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"golang.org/x/sync/singleflight"
)

var usageReads singleflight.Group

const usageReadTimeout = 30 * time.Second

type usageReply struct {
	status int
	body   []byte
	sent   *coreauth.Auth // Shared read-only identity captured before the provider request.
}

// ReadUsage coalesces in-progress usage reads across quota polling and reset
// inventory. Claude callers use the same cedar_ember superset endpoint so both
// parsers can consume the shared response. Results are not cached after completion.
func ReadUsage(ctx context.Context, auth *coreauth.Auth, fetch func(context.Context) (int, []byte, error)) (int, []byte, error) {
	status, body, _, err := ReadUsageSent(ctx, auth, func(readCtx context.Context) (int, []byte, *coreauth.Auth, error) {
		status, body, err := fetch(readCtx)
		return status, body, nil, err
	})
	return status, body, err
}

// ReadUsageSent shares the same flights as ReadUsage, including the actual sent
// identity. Callers must not mutate sent; it belongs to the shared response.
func ReadUsageSent(ctx context.Context, auth *coreauth.Auth, fetch func(context.Context) (int, []byte, *coreauth.Auth, error)) (int, []byte, *coreauth.Auth, error) {
	key := strings.ToLower(auth.Provider) + "\x00" + auth.ID + "\x00" + strconv.FormatUint(auth.RegistrationEpoch, 10) + "\x00" + strconv.FormatUint(auth.CredentialVersion, 10)
	done := usageReads.DoChan(key, func() (any, error) {
		readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), usageReadTimeout)
		defer cancel()
		status, body, sent, err := fetch(readCtx)
		return usageReply{status: status, body: body, sent: sent}, err
	})
	select {
	case <-ctx.Done():
		return 0, nil, nil, ctx.Err()
	case result := <-done:
		if result.Val == nil {
			return 0, nil, nil, result.Err
		}
		reply := result.Val.(usageReply)
		return reply.status, append([]byte(nil), reply.body...), reply.sent, result.Err
	}
}

// ReadUsageFresh never joins or publishes a shared read. Verification after a
// mutation must observe a request started after that mutation completed.
func ReadUsageFresh(ctx context.Context, _ *coreauth.Auth, fetch func(context.Context) (int, []byte, error)) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, usageReadTimeout)
	defer cancel()
	return fetch(ctx)
}

// Positive, stable jitter separates scheduled polls without affecting PollNow's
// minimum gap. Up to 10% of the cadence is added per account and interval.
func pollJitter(id string, every time.Duration) time.Duration {
	if every <= 0 {
		return 0
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(id))
	return time.Duration(hash.Sum64() % uint64(every/10+1))
}

func (s *Service) reservePollLocked(a *account) bool {
	if a.polling != nil {
		return false
	}
	a.polling = make(chan struct{})
	return true
}
func (s *Service) finishPoll(a *account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(a.polling)
	a.polling = nil
}
