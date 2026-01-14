package storage

import (
	"fmt"
	"strings"
)

// NormalizeKey converts a dimension key value to a canonical string form,
// suitable for in-memory cache keys (e.g. "Germany" or "8429529").
//
// Backends must not assume a particular underlying type for keys; this helper
// keeps lookup caches consistent across backends.
func NormalizeKey(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(t)
	case int64:
		return fmt.Sprintf("%d", t)
	case []byte:
		return strings.TrimSpace(string(t))
	case int:
		return fmt.Sprintf("%d", t)
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}
