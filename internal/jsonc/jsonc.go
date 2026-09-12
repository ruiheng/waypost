// Package jsonc normalizes the JSONC dialect Devin accepts for user-level
// configuration (// and /* */ comments, trailing commas) into strict JSON.
// JSON5 extensions Devin also tolerates, such as single-quoted strings or
// unquoted keys, are rejected so callers surface a clear parse error instead
// of silently misreading the file.
package jsonc

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Normalize prepares contents for encoding/json. Input that is already valid
// JSON is returned unchanged; JSONC input is standardized. The second return
// value reports whether normalization ran, so callers can warn that a
// rewritten file will lose comments.
func Normalize(contents []byte) (normalized []byte, changed bool, err error) {
	if json.Valid(contents) {
		return contents, false, nil
	}
	normalized, err = Standardize(contents)
	if err != nil {
		return nil, false, err
	}
	return normalized, true, nil
}

// Standardize strips comments and trailing commas from JSONC contents while
// preserving double-quoted string contents byte-for-byte. Newlines inside
// comments are kept so downstream parse errors still report useful line
// numbers.
func Standardize(contents []byte) ([]byte, error) {
	withoutComments, err := stripComments(contents)
	if err != nil {
		return nil, err
	}
	return stripTrailingCommas(withoutComments), nil
}

func stripComments(in []byte) ([]byte, error) {
	var out bytes.Buffer
	out.Grow(len(in))
	for i := 0; i < len(in); {
		switch in[i] {
		case '"':
			out.WriteByte(in[i])
			i++
			for i < len(in) {
				c := in[i]
				out.WriteByte(c)
				i++
				if c == '\\' && i < len(in) {
					out.WriteByte(in[i])
					i++
					continue
				}
				if c == '"' {
					break
				}
			}
		case '\'':
			return nil, fmt.Errorf("jsonc: single-quoted strings are not supported (byte %d)", i)
		case '/':
			if i+1 >= len(in) {
				out.WriteByte(in[i])
				i++
				continue
			}
			switch in[i+1] {
			case '/':
				i += 2
				for i < len(in) && in[i] != '\n' {
					i++
				}
			case '*':
				end := bytes.Index(in[i+2:], []byte("*/"))
				if end < 0 {
					return nil, fmt.Errorf("jsonc: unterminated block comment (byte %d)", i)
				}
				// Keep comment newlines so positions stay stable.
				for _, c := range in[i+2 : i+2+end] {
					if c == '\n' {
						out.WriteByte('\n')
					}
				}
				i += 2 + end + 2
			default:
				out.WriteByte(in[i])
				i++
			}
		default:
			out.WriteByte(in[i])
			i++
		}
	}
	return out.Bytes(), nil
}

func stripTrailingCommas(in []byte) []byte {
	var out bytes.Buffer
	out.Grow(len(in))
	inString := false
	for i := 0; i < len(in); i++ {
		c := in[i]
		switch {
		case inString:
			out.WriteByte(c)
			if c == '\\' && i+1 < len(in) {
				out.WriteByte(in[i+1])
				i++
				continue
			}
			if c == '"' {
				inString = false
			}
		case c == '"':
			inString = true
			out.WriteByte(c)
		case c == ',':
			j := i + 1
			for j < len(in) && (in[j] == ' ' || in[j] == '\t' || in[j] == '\n' || in[j] == '\r') {
				j++
			}
			if j < len(in) && (in[j] == '}' || in[j] == ']') {
				continue
			}
			out.WriteByte(c)
		default:
			out.WriteByte(c)
		}
	}
	return out.Bytes()
}
