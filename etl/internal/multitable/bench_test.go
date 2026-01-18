package multitable

import "testing"

// BenchmarkRepoKey measures repoKey string formation costs.
func BenchmarkRepoKey(b *testing.B) {
	cols := []string{"a", "b", "c", "d", "e", "f", "g"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = repoKey("table", cols)
	}
}
