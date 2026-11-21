package quota

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/MoeForge/moego/json"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/misc"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// Use CPAMC's primary quota endpoint, not the model catalog. No credit fallback
// or subscription lookup is needed to read quota windows.
var antigravitySpec = providerSpec{
	usageURL: "https://daily-cloudcode-pa.googleapis.com/v1internal:retrieveUserQuotaSummary",
	method:   "POST",
	bodyFor:  antigravityBody,
	headers: func(*coreauth.Auth) map[string]string {
		return map[string]string{
			"Authorization": "Bearer $TOKEN$",
			"Content-Type":  "application/json",
			"User-Agent":    misc.AntigravityUserAgent(),
		}
	},
	passive:    func(map[string]string, time.Time) []Window { return nil },
	parseUsage: ParseAntigravityUsage,
}

func antigravityBody(auth *coreauth.Auth) ([]byte, error) {
	project := ""
	if auth != nil {
		for _, key := range []string{"project_id", "projectId"} {
			if value, ok := auth.Metadata[key].(string); ok && strings.TrimSpace(value) != "" {
				project = strings.TrimSpace(value)
				break
			}
		}
		if project == "" {
			for _, key := range []string{"project_id", "projectId", "gemini_virtual_project"} {
				if value := strings.TrimSpace(auth.Attributes[key]); value != "" {
					project = value
					break
				}
			}
		}
	}
	if project == "" {
		return nil, errors.New("antigravity quota: missing project ID")
	}
	return json.Marshal(struct {
		Project string `json:"project"`
	}{project})
}

type antigravityBucket struct {
	ID             string          `json:"bucketId"`
	IDSnake        string          `json:"bucket_id"`
	Window         string          `json:"window"`
	Remaining      json.RawMessage `json:"remainingFraction"`
	RemainingSnake json.RawMessage `json:"remaining_fraction"`
	Reset          *string         `json:"resetTime"`
	ResetSnake     *string         `json:"reset_time"`
}

// ParseAntigravityUsage consumes CPAMC's groups/buckets quota contract. Each
// group and bucket keeps its own identity and reset. The response supplies no
// plan; CPAMC gets subscription metadata separately. Buckets without fractions
// are skipped; no usable buckets is an error that preserves earlier evidence.
func ParseAntigravityUsage(body []byte) ([]Window, string, error) {
	var payload struct {
		Groups []struct {
			Name      string              `json:"displayName"`
			NameSnake string              `json:"display_name"`
			Buckets   []antigravityBucket `json:"buckets"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, "", errors.New("antigravity quota: malformed groups payload")
	}
	var windows []Window
	seen := map[string]bool{}
	for gi, group := range payload.Groups {
		name := strings.TrimSpace(group.Name)
		if name == "" {
			name = strings.TrimSpace(group.NameSnake)
		}
		if name == "" {
			name = fmt.Sprintf("group-%d", gi+1)
		}
		for bi, bucket := range group.Buckets {
			raw := bucket.Remaining
			if len(raw) == 0 || string(raw) == "null" {
				raw = bucket.RemainingSnake
			}
			if len(raw) == 0 || string(raw) == "null" {
				continue
			}
			var value any
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, "", errors.New("antigravity quota: malformed remaining fraction")
			}
			remaining := math.NaN()
			switch v := value.(type) {
			case float64:
				remaining = v
			case string:
				if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
					remaining = f
				}
			}
			if math.IsNaN(remaining) || math.IsInf(remaining, 0) || remaining < 0 || remaining > 1 {
				return nil, "", errors.New("antigravity quota: invalid remaining fraction")
			}
			id := strings.TrimSpace(bucket.ID)
			if id == "" {
				id = strings.TrimSpace(bucket.IDSnake)
			}
			if id == "" {
				id = fmt.Sprintf("bucket-%d", bi+1)
			}
			period, duration := antigravityPeriod(bucket.Window)
			key := period + ":" + url.QueryEscape(name) + ":" + url.QueryEscape(id)
			if seen[key] {
				return nil, "", errors.New("antigravity quota: duplicate group/bucket identity")
			}
			seen[key] = true
			used := (1 - remaining) * 100
			w := Window{Window: key, UsedPct: &used}
			reset := bucket.Reset
			if reset == nil {
				reset = bucket.ResetSnake
			}
			if reset != nil && strings.TrimSpace(*reset) != "" {
				t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*reset))
				if err != nil {
					return nil, "", errors.New("antigravity quota: invalid reset time")
				}
				t = t.UTC()
				w.ResetsAt = &t
				if duration > 0 {
					start := t.Add(-duration)
					w.StartsAt = &start
				}
			}
			windows = append(windows, w)
		}
	}
	if len(windows) == 0 {
		return nil, "", errors.New("antigravity quota: no quota buckets")
	}
	sortWindows(windows)
	return windows, "", nil
}

// Only explicit window spellings in CPAMC's contract imply a duration. A reset
// alone cannot tell us the window's start or pace.
func antigravityPeriod(window string) (string, time.Duration) {
	switch strings.ToLower(strings.TrimSpace(window)) {
	case "5h", "five-hour", "five_hour":
		return "5h", 5 * time.Hour
	case "weekly", "week":
		return "7d", 7 * 24 * time.Hour
	default:
		return "quota/" + url.QueryEscape(strings.TrimSpace(window)), 0
	}
}
