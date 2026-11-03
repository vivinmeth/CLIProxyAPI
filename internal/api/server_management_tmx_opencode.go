package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/opencode"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/quota"
)

// OpenCode Go quota settings (tokenmaxx fork; internal/tmx/opencode): the plan and its per-model
// limits and pool, per openai-compatibility entry. Readings appear in /tmx/quota/current as accounts
// opencode:<entry>.
//
//	GET    /tmx/opencode/quota            every stored subscription, with its live reading (per-model use)
//	PUT    /tmx/opencode/quota            one subscription (body: opencode.Settings), replaced
//	DELETE /tmx/opencode/quota?entry=…    drop one
func registerTMXOpenCodeRoutes(v8 *gin.RouterGroup) {
	v8.GET("/tmx/opencode/quota", func(c *gin.Context) {
		st := tmxOpenCodeStore(c)
		if st == nil {
			return
		}
		subs, err := st.List(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if subs == nil {
			subs = []opencode.Settings{}
		}
		readings := make([]opencode.Reading, 0, len(subs))
		for _, sub := range subs {
			r, errR := st.Read(c.Request.Context(), sub, time.Now())
			if errR != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": errR.Error()})
				return
			}
			readings = append(readings, r)
		}
		c.JSON(http.StatusOK, gin.H{"subscriptions": subs, "readings": readings, "default_windows": opencode.DefaultWindows})
	})
	v8.PUT("/tmx/opencode/quota", func(c *gin.Context) {
		st := tmxOpenCodeStore(c)
		if st == nil {
			return
		}
		var body opencode.Settings
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body: " + err.Error()})
			return
		}
		saved, err := st.Put(c.Request.Context(), body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, saved)
	})
	v8.DELETE("/tmx/opencode/quota", func(c *gin.Context) {
		st := tmxOpenCodeStore(c)
		if st == nil {
			return
		}
		entry := strings.TrimSpace(c.Query("entry"))
		if entry == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "entry is required"})
			return
		}
		ok, err := st.Delete(c.Request.Context(), entry)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"deleted": ok})
	})
}

func tmxOpenCodeStore(c *gin.Context) *opencode.Store {
	st := quota.Default().OpenCode()
	if st == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "quota tracking is not running"})
	}
	return st
}
