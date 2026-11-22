package quota

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/MoeForge/moego/json"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// XAIBillingFetcher reads the fixed billing endpoint with the proxy's current
// OAuth snapshot. It must never acquire or refresh credentials, or retry a read.
// Keeping it separate prevents falling back to the refresh-capable Fetcher.
type XAIBillingFetcher interface {
	TmxXAIBilling(context.Context, *coreauth.Auth) (int, []byte, *coreauth.Auth, error)
}

var xaiSpec = providerSpec{
	passive:    func(map[string]string, time.Time) []Window { return nil },
	parseUsage: ParseXAIBilling,
}

// xaiCent matches the vendor's i64 Cent. An existing {} is proto3 zero;
// absent/null Cent objects remain nil. An explicit null val is not a number.
type xaiCent struct{ Val int64 }

func (c *xaiCent) UnmarshalJSON(b []byte) error {
	var v struct {
		Val json.RawMessage `json:"val"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	if len(v.Val) == 0 {
		c.Val = 0
		return nil
	}
	if string(v.Val) == "null" {
		return errors.New("null cent value")
	}
	return json.Unmarshal(v.Val, &c.Val)
}

type xaiPeriod struct {
	Type  *string `json:"type"`
	Start *string `json:"start"`
	End   *string `json:"end"`
}

// ParseXAIBilling follows xai-org/grok-build 2bdd1d6 billing.rs. Included
// credits are independent of onDemandCap/onDemandUsed/prepaidBalance. Neither
// those amounts, productUsage, nor request/token limits define this window.
func ParseXAIBilling(body []byte) ([]Window, string, error) {
	var p struct {
		Config *struct {
			CreditUsagePercent *float64   `json:"creditUsagePercent"`
			CurrentPeriod      *xaiPeriod `json:"currentPeriod"`
			MonthlyLimit       *xaiCent   `json:"monthlyLimit"`
			Used               *xaiCent   `json:"used"`
			BillingPeriodStart *string    `json:"billingPeriodStart"`
			BillingPeriodEnd   *string    `json:"billingPeriodEnd"`
		} `json:"config"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, "", errors.New("xai billing: malformed config")
	}
	if p.Config == nil {
		return nil, "", errors.New("xai billing: missing config")
	}
	c := p.Config
	w := Window{Window: "billing"}
	if c.Used != nil && c.Used.Val < 0 || c.MonthlyLimit != nil && c.MonthlyLimit.Val < 0 {
		return nil, "", errors.New("xai billing: negative included credits")
	}
	if c.CreditUsagePercent != nil {
		value := *c.CreditUsagePercent
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 100 {
			return nil, "", errors.New("xai billing: invalid credit percentage")
		}
		w.UsedPct = &value
	} else if c.Used != nil && c.MonthlyLimit != nil && c.MonthlyLimit.Val > 0 {
		value := float64(c.Used.Val) / float64(c.MonthlyLimit.Val) * 100
		w.UsedPct = &value
	}
	start, end := c.BillingPeriodStart, c.BillingPeriodEnd
	typed := false
	if period := c.CurrentPeriod; period != nil {
		start, end = period.Start, period.End
		if period.Type != nil {
			switch *period.Type {
			case "USAGE_PERIOD_TYPE_WEEKLY":
				w.Window = "7d"
				typed = true
			case "USAGE_PERIOD_TYPE_MONTHLY":
				w.Window = "monthly"
				typed = true
			}
		}
	}
	var err error
	if w.StartsAt, err = xaiBillingTime(start); err != nil {
		return nil, "", err
	}
	if w.ResetsAt, err = xaiBillingTime(end); err != nil {
		return nil, "", err
	}
	if w.StartsAt != nil && w.ResetsAt != nil && !w.ResetsAt.After(*w.StartsAt) {
		return nil, "", errors.New("xai billing: invalid period ordering")
	}
	if w.UsedPct == nil && w.StartsAt == nil && w.ResetsAt == nil && !typed {
		return nil, "", errors.New("xai billing: no included credit evidence")
	}
	return []Window{w}, "", nil
}

func xaiBillingTime(value *string) (*time.Time, error) {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, *value)
	if err != nil || t.IsZero() {
		return nil, errors.New("xai billing: invalid period timestamp")
	}
	t = t.UTC()
	return &t, nil
}

// A shared read can finish after its original waiter was canceled. Validate
// its identity again at the scheduler before recording or stopping an account.
func (s *Service) checkXAIBillingSent(expected, sent *coreauth.Auth) error {
	if sent != nil && sent.ID == expected.ID && sent.RegistrationEpoch == expected.RegistrationEpoch && sent.CredentialVersion == expected.CredentialVersion {
		for _, current := range s.fetch.TmxAuths() {
			if current != nil && current.ID == sent.ID && current.RegistrationEpoch == sent.RegistrationEpoch && current.CredentialVersion == sent.CredentialVersion && strings.EqualFold(strings.TrimSpace(current.Provider), "xai") && !current.Disabled && current.Status != coreauth.StatusDisabled {
				return nil
			}
		}
	}
	return errors.New("xai billing: current sent identity unavailable or changed")
}

// A billing 401 is a stop, not an invitation to refresh or retry an hour later.
// Use the existing registration-bound stop so normal token rotation cannot
// silently rearm it. Recovery requires explicit re-registration or restart.
func (s *Service) stopXAIBilling(a *account, sent *coreauth.Auth, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a.LastPollAt = &now
	a.PollError = "unauthorized (401): xAI billing polling stopped; re-register account after resolving authentication"
	a.pauseUntil = time.Time{}
	a.refused = &quotaAuthIdentity{sent.RegistrationEpoch, sent.CredentialVersion}
}
