package api

// tokenmaxx fork: GET /v8/management/tmx/durability, the writers' durability counters
// (internal/tmx/durability). Read-only.

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/durability"
)

func registerTMXDurabilityRoutes(v8 *gin.RouterGroup) {
	v8.GET("/tmx/durability", tmxDurability)
}

type tmxDurabilityTotals struct {
	Queued          int64 `json:"queued"`
	Spooled         int64 `json:"spooled"`
	SpooledPending  int64 `json:"spooled_pending"`
	SpooledBytes    int64 `json:"spooled_bytes"`
	Replayed        int64 `json:"replayed"`
	Dropped         int64 `json:"dropped"`
	Deadlettered    int64 `json:"deadlettered"`
	DeadletterTotal int64 `json:"deadletter_total"`
}

// tmxDurability: GET /tmx/durability — per-writer queue / spool / drop / dead-letter counters.
// lossless is false as soon as any writer dropped an item since start.
func tmxDurability(c *gin.Context) {
	writers := durability.Snapshot()
	var t tmxDurabilityTotals
	for _, w := range writers {
		t.Queued += w.Queued
		t.Spooled += w.Spooled
		t.SpooledPending += w.SpooledPending
		t.SpooledBytes += w.SpooledBytes
		t.Replayed += w.Replayed
		t.Dropped += w.Dropped
		t.Deadlettered += w.Deadlettered
		t.DeadletterTotal += w.DeadletterTotal
	}
	c.JSON(http.StatusOK, gin.H{
		"generated_at": time.Now().UTC(),
		"lossless":     t.Dropped == 0,
		"totals":       t,
		"writers":      writers,
	})
}
