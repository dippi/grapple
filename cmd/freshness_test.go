package cmd

import (
	"testing"
	"time"
)

func TestParseFreshness(t *testing.T) {
	cases := []struct {
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{"1d", 24 * time.Hour, false},
		{"1d12h30m", 24*time.Hour + 12*time.Hour + 30*time.Minute, false},
		{"2h", 2 * time.Hour, false},
		{"", 0, true},
		{"xd", 0, true},
		{"1dxyz", 0, true},
	}

	for _, c := range cases {
		dur, err := parseFreshness(c.input)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseFreshness(%q) expected error, got nil", c.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseFreshness(%q) unexpected error: %v", c.input, err)
			continue
		}
		if dur != c.expected {
			t.Errorf("parseFreshness(%q) = %v, want %v", c.input, dur, c.expected)
		}
	}
}
