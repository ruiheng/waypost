package jsonc

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNormalizeValidJSONPassesThrough(t *testing.T) {
	contents := []byte("{\n  \"a\": 1,\n  \"b\": \"// inside a string\"\n}")
	normalized, changed, err := Normalize(contents)
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if changed {
		t.Fatal("Normalize() changed = true, want false for valid JSON")
	}
	if string(normalized) != string(contents) {
		t.Fatalf("Normalize() = %q, want input unchanged", normalized)
	}
}

func TestStandardizeStripsCommentsAndTrailingCommas(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "line comment",
			in:   "{\n// comment\n\"a\": 1\n}",
			want: "{\n\n\"a\": 1\n}",
		},
		{
			name: "trailing line comment",
			in:   `{"a": 1} // done`,
			want: `{"a": 1} `,
		},
		{
			name: "block comment",
			in:   `{/* before */"a": 1}`,
			want: `{"a": 1}`,
		},
		{
			name: "block comment keeps newlines",
			in:   "{\n/* one\ntwo */\n\"a\": 1}",
			want: "{\n\n\n\"a\": 1}",
		},
		{
			name: "trailing comma object",
			in:   `{"a": 1,}`,
			want: `{"a": 1}`,
		},
		{
			name: "trailing comma array",
			in:   `{"a": [1, 2, ],}`,
			want: `{"a": [1, 2 ]}`,
		},
		{
			name: "comma with whitespace before brace",
			in:   "{\"a\": 1,\n \t}",
			want: "{\"a\": 1\n \t}",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Standardize([]byte(tc.in))
			if err != nil {
				t.Fatalf("Standardize() error = %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("Standardize() = %q, want %q", got, tc.want)
			}
			if !json.Valid(got) {
				t.Fatalf("Standardize() = %q is not valid JSON", got)
			}
		})
	}
}

func TestStandardizePreservesCommentLikeStrings(t *testing.T) {
	in := `{"url": "https://example.com/a//b", "pat": "a /* b */ c", "esc": "a\" // tail"}`
	got, err := Standardize([]byte(in))
	if err != nil {
		t.Fatalf("Standardize() error = %v", err)
	}
	var parsed map[string]string
	if err := json.Unmarshal(got, &parsed); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if parsed["url"] != "https://example.com/a//b" || parsed["pat"] != "a /* b */ c" || parsed["esc"] != `a" // tail` {
		t.Fatalf("parsed = %#v, want string contents preserved", parsed)
	}
}

func TestStandardizeRejectsUnsupportedSyntax(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      string
		wantErr string
	}{
		{name: "single-quoted string", in: `{'a': 1}`, wantErr: "single-quoted"},
		{name: "unterminated block comment", in: `{/* x`, wantErr: "unterminated block comment"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Standardize([]byte(tc.in))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Standardize() error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestNormalizeMarksJSONC(t *testing.T) {
	normalized, changed, err := Normalize([]byte("{\n// c\n\"a\": 1,\n}"))
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if !changed {
		t.Fatal("Normalize() changed = false, want true for JSONC input")
	}
	if !json.Valid(normalized) {
		t.Fatalf("Normalize() = %q is not valid JSON", normalized)
	}
}

func TestNormalizePropagatesStandardizeError(t *testing.T) {
	if _, _, err := Normalize([]byte(`{'a': 1}`)); err == nil {
		t.Fatal("Normalize() error = nil, want single-quote rejection")
	}
}
