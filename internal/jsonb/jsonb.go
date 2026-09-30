// Package jsonb checks whether JSON can be stored in a PostgreSQL jsonb
// column.
//
// jsonb is stricter than encoding/json. It rejects three things that
// json.Valid accepts:
//
//   - invalid UTF-8 bytes (the server refuses them for a UTF8 database);
//   - the \u0000 escape (text values cannot contain NUL);
//   - unpaired UTF-16 surrogate escapes, such as a lone "\ud800".
//
// Checking first lets callers answer 400 for bad input, or store bad output
// in a safe form, instead of failing inside the database.
package jsonb

import (
	"encoding/json"
	"unicode/utf8"
)

// Storable reports whether data is JSON that a jsonb column accepts.
func Storable(data []byte) bool {
	if !json.Valid(data) || !utf8.Valid(data) {
		return false
	}
	return escapesStorable(data)
}

// escapesStorable scans the \u escapes of valid JSON and rejects \u0000 and
// unpaired surrogates. It relies on data being valid JSON, so every
// backslash starts a well-formed escape.
func escapesStorable(data []byte) bool {
	inString := false
	pendingHigh := false // the previous escape was a high surrogate
	for i := 0; i < len(data); i++ {
		c := data[i]
		if !inString {
			if c == '"' {
				inString = true
			}
			continue
		}
		switch c {
		case '"':
			if pendingHigh {
				return false // high surrogate at the end of a string
			}
			inString = false
		case '\\':
			i++ // data[i] is the escape letter
			if data[i] != 'u' {
				if pendingHigh {
					return false
				}
				continue
			}
			r := hex4(data[i+1 : i+5])
			i += 4
			switch {
			case r == 0:
				return false
			case r >= 0xD800 && r <= 0xDBFF: // high surrogate
				if pendingHigh {
					return false
				}
				pendingHigh = true
			case r >= 0xDC00 && r <= 0xDFFF: // low surrogate
				if !pendingHigh {
					return false
				}
				pendingHigh = false
			default:
				if pendingHigh {
					return false
				}
			}
		default:
			if pendingHigh {
				return false // a high surrogate must be followed by its low half
			}
		}
	}
	return true
}

// hex4 parses four hex digits; valid JSON guarantees they are hex.
func hex4(b []byte) rune {
	var r rune
	for _, c := range b {
		r <<= 4
		switch {
		case c >= '0' && c <= '9':
			r |= rune(c - '0')
		case c >= 'a' && c <= 'f':
			r |= rune(c - 'a' + 10)
		case c >= 'A' && c <= 'F':
			r |= rune(c - 'A' + 10)
		}
	}
	return r
}
