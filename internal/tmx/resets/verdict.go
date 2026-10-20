package resets

import (
	"math"
	"sort"
	"time"
)

const (
	// LapseSoon flags credits expiring within this.
	LapseSoon = 72 * time.Hour
	// useNowMinWait: an exhausted window refilling later than this makes spending urgent.
	useNowMinWait = 12 * time.Hour
)

// Verdict kinds, in the order they are decided.
const (
	VerdictPending   = "pending"     // an earlier claim has no definite answer: retry it (same id)
	VerdictBlocked   = "blocked"     // the provider would refuse it now (see Credit.Blocker)
	VerdictNothing   = "nothing"     // nothing to clear right now
	VerdictUseNow    = "use_now"     // a cleared window is exhausted and refills > 12 h from now
	VerdictUseOrLose = "use_or_lose" // something to clear, and it expires before its windows refill
	VerdictHold      = "hold"        // outlives the next refill: keep it
)

// Gain is what spending a credit now returns for one window, in percentage points.
type Gain struct {
	Window  string  `json:"window"`
	UsedPct float64 `json:"used_pct"`
}

// Verdict is the server's recommendation for one credit, computed at response time.
type Verdict struct {
	Kind               string     `json:"kind"`
	Gains              []Gain     `json:"gains"`
	TotalGain          float64    `json:"total_gain"`
	RefillAt           *time.Time `json:"refill_at,omitempty"` // latest natural refill among cleared windows
	LapsesBeforeRefill bool       `json:"lapses_before_refill"`
	LapsesSoon         bool       `json:"lapses_soon"` // expires within 72 h
	// Spendable: this is the credit a redeem should target (soonest expiry without a blocker).
	Spendable bool `json:"spendable"`
}

// Summary aggregates all listed accounts.
type Summary struct {
	Banked              int         `json:"banked"`
	LapseSoon           int         `json:"lapse_soon"`
	AccountsWithCredits int         `json:"accounts_with_credits"`
	Pending             int         `json:"pending"`
	NextExpiry          *NextExpiry `json:"next_expiry"`
}

// NextExpiry is the soonest-expiring credit across all accounts.
type NextExpiry struct {
	At       time.Time `json:"at"`
	AuthID   string    `json:"auth_id"`
	Provider string    `json:"provider"`
	CreditID string    `json:"credit_id"`
}

// SpendableCredit is the credit a redeem should target: soonest expiry among credits without a
// blocker; ties go to the provider's recommendation, then id.
func SpendableCredit(a Account) *Credit {
	var usable []Credit
	for _, c := range a.Credits {
		if c.Blocker == "" {
			usable = append(usable, c)
		}
	}
	if len(usable) == 0 {
		return nil
	}
	sort.SliceStable(usable, func(i, j int) bool {
		ei, ej := usable[i].ExpiresAt, usable[j].ExpiresAt
		switch {
		case ei != nil && ej != nil && !ei.Equal(*ej):
			return ei.Before(*ej)
		case ei != nil && ej == nil:
			return true
		case ei == nil && ej != nil:
			return false
		}
		if usable[i].ID == a.NextCreditID {
			return true
		}
		if usable[j].ID == a.NextCreditID {
			return false
		}
		return usable[i].ID < usable[j].ID
	})
	c := usable[0]
	return &c
}

// creditVerdict decides one credit's verdict.
func creditVerdict(a Account, c Credit, spendableID string, now time.Time) *Verdict {
	clears := map[string]bool{}
	for _, w := range c.Clears {
		clears[w] = true
	}
	v := &Verdict{Gains: []Gain{}, Spendable: c.ID == spendableID}
	exhausted := false
	for _, w := range a.Windows {
		if len(clears) > 0 && !clears[w.Window] {
			continue
		}
		used := 0.0
		if w.UsedPct != nil {
			used = *w.UsedPct
		}
		if used > 0 {
			g := Gain{Window: w.Window, UsedPct: math.Round(used)}
			v.Gains = append(v.Gains, g)
			v.TotalGain += g.UsedPct
		}
		if w.ResetsAt != nil && w.ResetsAt.After(now) && (v.RefillAt == nil || w.ResetsAt.After(*v.RefillAt)) {
			t := *w.ResetsAt
			v.RefillAt = &t
		}
		if used >= 99 && (w.ResetsAt == nil || w.ResetsAt.Sub(now) > useNowMinWait) {
			exhausted = true
		}
	}
	if c.ExpiresAt != nil {
		v.LapsesBeforeRefill = v.RefillAt != nil && c.ExpiresAt.Before(*v.RefillAt)
		v.LapsesSoon = c.ExpiresAt.After(now) && c.ExpiresAt.Sub(now) <= LapseSoon
	}
	switch {
	case a.Pending != nil && a.Pending.CreditID == c.ID:
		v.Kind = VerdictPending
	case c.Blocker != "":
		v.Kind = VerdictBlocked
	case v.TotalGain == 0 || c.Warning == "nothing_to_clear":
		v.Kind = VerdictNothing
	case exhausted:
		v.Kind = VerdictUseNow
	case v.LapsesBeforeRefill:
		v.Kind = VerdictUseOrLose
	default:
		v.Kind = VerdictHold
	}
	return v
}

// decorate fills every credit's verdict. The account must own its Credits slice (see own).
func decorate(a *Account, now time.Time) {
	spendableID := ""
	if s := SpendableCredit(*a); s != nil {
		spendableID = s.ID
	}
	for i := range a.Credits {
		a.Credits[i].Verdict = creditVerdict(*a, a.Credits[i], spendableID, now)
	}
}

// own copies the slices a cached account shares, so a response can be decorated safely.
func own(a Account) Account {
	a.Windows = append([]Window{}, a.Windows...)
	a.Credits = append([]Credit{}, a.Credits...)
	return a
}

// Summarize aggregates accounts for the summary cell.
func Summarize(accounts []Account, now time.Time) Summary {
	var s Summary
	for _, a := range accounts {
		if a.Pending != nil {
			s.Pending++
		}
		live := 0
		for _, c := range a.Credits {
			if c.Blocker == "expired" {
				continue
			}
			live++
			if c.ResetsLeft > 1 {
				s.Banked += c.ResetsLeft
			} else {
				s.Banked++
			}
			if c.ExpiresAt == nil || !c.ExpiresAt.After(now) {
				continue
			}
			if c.ExpiresAt.Sub(now) <= LapseSoon {
				s.LapseSoon++
			}
			if s.NextExpiry == nil || c.ExpiresAt.Before(s.NextExpiry.At) {
				s.NextExpiry = &NextExpiry{At: *c.ExpiresAt, AuthID: a.AuthID, Provider: a.Provider, CreditID: c.ID}
			}
		}
		if live > 0 {
			s.AccountsWithCredits++
		}
	}
	return s
}
