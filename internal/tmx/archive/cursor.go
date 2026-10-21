package archive

import (
	"encoding/base64"
	"errors"
	"strings"
	"time"
)

// ErrBadCursor is returned for a cursor a list call didn't produce.
var ErrBadCursor = errors.New("invalid cursor")

// Keyset cursors for the newest-first lists: the last row's (time, id), opaque to callers.
func encodeCursor(t time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano) + "|" + id))
}

// decodeCursor returns the zero time and "" for an empty cursor (first page).
func decodeCursor(cursor string) (time.Time, string, error) {
	if cursor == "" {
		return time.Time{}, "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	ts, id, ok := strings.Cut(string(raw), "|")
	if err != nil || !ok || id == "" {
		return time.Time{}, "", ErrBadCursor
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}, "", ErrBadCursor
	}
	return t, id, nil
}
