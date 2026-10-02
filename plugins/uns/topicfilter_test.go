package uns

import "testing"

func TestMatchFilter(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b", false},
		{"a/b", "a/b/c", false},
		{"a/+/c", "a/x/c", true},
		{"a/+/c", "a/x/y/c", false},
		{"a/#", "a", true},
		{"a/#", "a/b/c", true},
		{"a/+", "a", false},
		{"#", "a/b", true},
		{"a/b/#", "a/c", false},
	}
	for _, c := range cases {
		if got := MatchFilter(c.filter, c.topic); got != c.want {
			t.Errorf("MatchFilter(%q, %q) = %v, want %v", c.filter, c.topic, got, c.want)
		}
	}
}

func TestValidFilter(t *testing.T) {
	for f, want := range map[string]bool{
		"a/b": true, "a/+/c": true, "a/#": true, "#": true,
		"": false, "a/#/b": false, "a/b+": false, "a#": false,
	} {
		if got := ValidFilter(f); got != want {
			t.Errorf("ValidFilter(%q) = %v, want %v", f, got, want)
		}
	}
}
