package archive

import (
	"context"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// R2 list prices (USD) used for the estimate. Free tiers are ignored on purpose: they are shared
// with every other bucket on the account, so the estimate stays an upper bound.
const (
	r2StoragePerGBMonth = 0.015
	r2WritesPerMillion  = 4.50
	costCacheTTL        = 10 * time.Minute
	costWarnInterval    = 6 * time.Hour
	defaultCostWarnUSD  = 15.0
)

// CostEstimate is what this environment's archive costs on R2 now and in 12 months if the last
// 30 days' growth continues.
type CostEstimate struct {
	StoredGB       float64   `json:"stored_gb"`
	GrowthGB30d    float64   `json:"growth_gb_30d"`
	Writes30d      int64     `json:"writes_30d"`
	MonthUSDNow    float64   `json:"month_usd_now"`
	MonthUSDIn12Mo float64   `json:"month_usd_in_12mo"`
	WarnUSD        float64   `json:"warn_usd"`
	Warning        bool      `json:"warning"`
	ComputedAt     time.Time `json:"computed_at"`
}

type costTracker struct {
	mu       sync.Mutex
	last     *CostEstimate
	lastWarn time.Time
}

func costWarnThreshold() float64 {
	if v, err := strconv.ParseFloat(strings.TrimSpace(os.Getenv("TMX_ARCHIVE_COST_WARN_USD")), 64); err == nil && v > 0 {
		return v
	}
	return defaultCostWarnUSD
}

// Cost returns a cached estimate, recomputing it at most every costCacheTTL.
func (a *Archive) Cost(ctx context.Context) (*CostEstimate, error) {
	a.cost.mu.Lock()
	defer a.cost.mu.Unlock()
	if a.cost.last != nil && time.Since(a.cost.last.ComputedAt) < costCacheTTL {
		return a.cost.last, nil
	}
	var stored, growth float64
	var writes int64
	if err := a.db.QueryRowContext(ctx, `SELECT
		coalesce(sum(stored_size), 0),
		coalesce(sum(stored_size) FILTER (WHERE created_at > now() - interval '30 days'), 0),
		(SELECT count(DISTINCT pack_key) FROM `+schema+`.blobs
			WHERE pack_key IS NOT NULL AND uploaded_at > now() - interval '30 days') +
		(SELECT count(*) FROM `+schema+`.blobs
			WHERE pack_key IS NULL AND uploaded_at > now() - interval '30 days')
		FROM `+schema+`.blobs`).Scan(&stored, &growth, &writes); err != nil {
		return nil, err
	}
	est := &CostEstimate{
		StoredGB:    round(stored / 1e9),
		GrowthGB30d: round(growth / 1e9),
		Writes30d:   writes,
		WarnUSD:     costWarnThreshold(),
		ComputedAt:  time.Now(),
	}
	writesUSD := float64(writes) / 1e6 * r2WritesPerMillion
	est.MonthUSDNow = round(est.StoredGB*r2StoragePerGBMonth + writesUSD)
	est.MonthUSDIn12Mo = round((est.StoredGB+12*est.GrowthGB30d)*r2StoragePerGBMonth + writesUSD)
	est.Warning = est.MonthUSDIn12Mo >= est.WarnUSD
	if est.Warning && time.Since(a.cost.lastWarn) > costWarnInterval {
		a.cost.lastWarn = time.Now()
		log.WithField("month_usd_in_12mo", est.MonthUSDIn12Mo).WithField("warn_usd", est.WarnUSD).
			Warn("tmx archive: projected R2 cost is over the warning threshold; set TMX_ARCHIVE_CAPTURE=off to stop body capture")
	}
	a.cost.last = est
	return est, nil
}

func round(v float64) float64 { return math.Round(v*1000) / 1000 }

// costWatch recomputes the estimate hourly so the warning is logged even if nobody opens the
// status endpoint.
func (a *Archive) costWatch() {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		if a.closing.Load() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if _, err := a.Cost(ctx); err != nil {
			log.WithError(err).Debug("tmx archive: cost estimate failed")
		}
		cancel()
	}
}
