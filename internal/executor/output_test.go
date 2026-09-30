package executor

import (
	"testing"

	"github.com/kaustubhagarwal21/flowd/internal/jsonb"
)

// Whatever a target answers, the step output must be storable in jsonb:
// otherwise saving it fails on every attempt.
func FuzzMarshalOutputIsStorable(f *testing.F) {
	for _, s := range []string{
		`{"a":1}`,
		"{\"a\":\"\x5cud83d\x5cude00\"}", // escaped surrogate pair (\x5c is a backslash)
		`{"a":"\ud800"}`,                 // lone surrogate escape
		`"x\u0000y"`,                     // NUL escape
		"{\"a\":\"caf\xe9\"}",            // invalid UTF-8 inside a JSON string
		"{\"a\":\"\xed\xa0\x80\"}",       // a surrogate encoded as UTF-8 bytes
		"GIF\x00\x01",
		"",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		out, err := marshalOutput(200, body)
		if err != nil {
			t.Fatalf("marshalOutput(%q): %v", body, err)
		}
		if !jsonb.Storable(out) {
			t.Errorf("marshalOutput(%q) = %q, which jsonb cannot store", body, out)
		}
	})
}
