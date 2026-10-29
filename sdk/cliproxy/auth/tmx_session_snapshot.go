package auth

import (
	"sort"
	"time"
)

// SessionBinding is one logical session, including every identifier alias.
// Expiration is absolute so a restart never extends the session TTL.
type SessionBinding struct {
	AuthID    string    `json:"auth_id"`
	Aliases   []string  `json:"aliases"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (c *SessionCache) Snapshot() []SessionBinding {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	out := make([]SessionBinding, 0, len(c.groups))
	for _, entry := range c.groups {
		if now.Before(entry.expiresAt) {
			out = append(out, SessionBinding{entry.authID, append([]string(nil), entry.aliases...), entry.expiresAt})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Aliases[0] < out[j].Aliases[0] })
	return out
}

// Restore merges valid bindings without replacing a newer live binding.
func (c *SessionCache) Restore(bindings []SessionBinding, allowed func(string) bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitializedLocked()
	now := time.Now()
	for _, binding := range bindings {
		if binding.AuthID == "" || !now.Before(binding.ExpiresAt) || (allowed != nil && !allowed(binding.AuthID)) {
			continue
		}
		aliases := compactSessionAliases(mergeSessionAliases(nil, binding.Aliases...))
		if len(aliases) == 0 {
			continue
		}
		previous := make([]sessionEntry, 0, len(aliases))
		newer := false
		// The snapshot doesn't carry the provider-aware binding; keep the newest one it replaces,
		// as Bind does for a selection without a session or provider.
		var restored sessionBinding
		for _, alias := range aliases {
			if e, ok := c.entries[alias]; ok {
				previous = append(previous, e)
				if e.expiresAt.After(binding.ExpiresAt) {
					newer = true
				}
				if e.binding.sequence > restored.sequence {
					restored = e.binding
				}
			}
		}
		if !newer {
			c.replaceAliasGroupsLocked(binding.AuthID, binding.ExpiresAt, aliases, restored, previous...)
		}
	}
}

func (m *Manager) SnapshotSessionBindings() []SessionBinding {
	if m == nil {
		return nil
	}
	if selector, ok := m.Selector().(*SessionAffinitySelector); ok {
		return selector.cache.Snapshot()
	}
	return nil
}

func (m *Manager) RestoreSessionBindings(bindings []SessionBinding) {
	if m == nil {
		return
	}
	if selector, ok := m.Selector().(*SessionAffinitySelector); ok {
		allowed := make(map[string]bool)
		for _, a := range m.List() {
			allowed[a.ID] = !a.Disabled && a.Status != StatusDisabled
		}
		selector.cache.Restore(bindings, func(id string) bool { return allowed[id] })
	}
}

func (m *Manager) BoundSessionCounts() map[string]int {
	counts := make(map[string]int)
	for _, binding := range m.SnapshotSessionBindings() {
		counts[binding.AuthID]++
	}
	return counts
}
