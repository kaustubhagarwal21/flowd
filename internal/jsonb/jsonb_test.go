package jsonb

import "testing"

// bs is a backslash. The escaped cases below build their \u escapes from it,
// so tools that decode \u escapes in source text cannot turn them into
// literal characters.
const bs = "\x5c"

func TestStorable(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"object", `{"a":1,"b":[true,null,"x"]}`, true},
		{"plain unicode", `"café ✓"`, true},
		{"escaped unicode", `"caf` + bs + `u00e9"`, true},
		{"surrogate pair", `"` + bs + `ud83d` + bs + `ude00"`, true},
		{"other escapes", `"a\nb\t\"c\\"`, true},
		{"escaped backslash then u", `"\\u0000"`, true}, // a literal backslash, not an escape
		{"invalid json", `{"a":`, false},
		{"invalid utf8", "\"caf\xe9\"", false},
		{"nul escape", `"a\u0000b"`, false},
		{"upper-case hex pair", `"` + bs + `uD83D` + bs + `uDE00"`, true},
		{"lone high surrogate", `"\ud800"`, false},
		{"lone low surrogate", `"\udc00"`, false},
		{"high then char", `"\ud800x"`, false},
		{"high then escape", `"\ud800\n"`, false},
		{"two highs", `"\ud800\ud800"`, false},
		{"nul in key", `{"\u0000":1}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Storable([]byte(tt.in)); got != tt.want {
				t.Errorf("Storable(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
