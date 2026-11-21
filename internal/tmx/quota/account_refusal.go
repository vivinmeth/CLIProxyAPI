package quota

import (
	"context"
	"errors"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

type quotaAuthIdentity struct{ epoch, version uint64 }

// The sent version attributes the stop, but token refreshes also change version.
// A fallback stop therefore survives all changes within its registration. Repair
// the hook/store, then re-register the account or restart the service to recover.
func (a *account) clearReplacedRefusal(auth *coreauth.Auth) {
	if a.refused != nil && a.refused.epoch != auth.RegistrationEpoch {
		a.refused = nil
	}
}

func (s *Service) refuseQuotaAccount(ctx context.Context, auth, sent *coreauth.Auth, a *account, code string, now time.Time) {
	refuser, ok := s.fetch.(AccountRefuser)
	var err error
	if ok && sent != nil {
		err = refuser.TmxRefuseAccount(ctx, sent, code)
	}
	stale := errors.Is(err, coreauth.ErrTmxRefusalStale)
	localStop := !ok || sent == nil || err != nil && !stale
	s.mu.Lock()
	a.LastPollAt = &now
	a.PollError = refusals.StatusMessage(code)
	a.pauseUntil = time.Time{}
	if stale {
		a.PollError = coreauth.ErrTmxRefusalStale.Error()
	}
	logStop := localStop && a.refused == nil
	if localStop {
		identity := auth
		if sent != nil {
			identity = sent
		}
		a.refused = &quotaAuthIdentity{identity.RegistrationEpoch, identity.CredentialVersion}
	}
	s.mu.Unlock()
	if logStop {
		log.WithField("auth_id", auth.ID).Error("tmx quota: terminal account refusal could not be persisted; polling stopped; repair hook/storage, then re-register account or restart service")
	}
}
