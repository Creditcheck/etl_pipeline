package multitable

import "testing"

func BenchmarkNormalizeKey_Int64(b *testing.B) {
	var v any = int64(1234567890)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = normalizeKey(v)
	}
}

func BenchmarkNormalizeKey_StringTrim(b *testing.B) {
	var v any = "  hello  "
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = normalizeKey(v)
	}
}
