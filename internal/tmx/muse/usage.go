package muse

import (
	"net/http"
	"strconv"

	"github.com/tidwall/gjson"
)

// SubscriptionHeaderPrefix marks the quota signals built from Meta's subscription usage. The
// auth manager keeps them per credential like Claude's rate-limit headers, and internal/tmx/quota
// reads them back as passive readings.
const SubscriptionHeaderPrefix = "X-Meta-Subscription-"

// SubscriptionUsageHeaders turns one response.subscription_usage event
//
//	{"type":"response.subscription_usage","subscription":{"tier":"…",
//	  "window":{"used_percent":10,"resets_at":1792272519,"window_duration_mins":300},
//	  "weekly":{"used_percent":80,"resets_at":1792368000}}}
//
// into X-Meta-Subscription-{Window,Weekly}-{Used-Percent,Reset-At,Window-Minutes} headers, or nil
// when the event carries no usage. The same object is "subs_usage" in the /muse-code/key answer,
// so that shape (no "subscription" wrapper) is accepted too.
func SubscriptionUsageHeaders(eventData []byte) http.Header {
	if len(eventData) == 0 || !gjson.ValidBytes(eventData) {
		return nil
	}
	root := gjson.GetBytes(eventData, "subscription")
	if !root.IsObject() {
		root = gjson.ParseBytes(eventData)
	}
	h := http.Header{}
	for _, slot := range []struct{ key, name string }{{"window", "Window"}, {"weekly", "Weekly"}} {
		w := root.Get(slot.key)
		if !w.IsObject() {
			continue
		}
		base := SubscriptionHeaderPrefix + slot.name + "-"
		if v := w.Get("used_percent"); v.Exists() && v.Type == gjson.Number {
			h.Set(base+"Used-Percent", strconv.FormatFloat(v.Float(), 'f', -1, 64))
		}
		if v := w.Get("resets_at").Int(); v > 0 {
			h.Set(base+"Reset-At", strconv.FormatInt(v, 10))
		}
		if v := w.Get("window_duration_mins").Int(); v > 0 {
			h.Set(base+"Window-Minutes", strconv.FormatInt(v, 10))
		}
	}
	if len(h) == 0 {
		return nil
	}
	return h
}
