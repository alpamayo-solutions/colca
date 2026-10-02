package uns

import "strings"

// ValidFilter reports whether f is an MQTT topic filter: "+" stands for one
// whole level, and "#" only for the last level.
func ValidFilter(f string) bool {
	if f == "" {
		return false
	}
	levels := strings.Split(f, "/")
	for i, level := range levels {
		if strings.ContainsAny(level, "+#") && level != "+" && level != "#" {
			return false
		}
		if level == "#" && i != len(levels)-1 {
			return false
		}
	}
	return true
}

// MatchFilter reports whether topic matches the MQTT topic filter f, which
// ValidFilter accepted.
func MatchFilter(f, topic string) bool {
	for {
		level, rest, more := strings.Cut(f, "/")
		if level == "#" {
			return true
		}
		tlevel, trest, tmore := strings.Cut(topic, "/")
		if level != "+" && level != tlevel {
			return false
		}
		if !more || !tmore {
			// "a/#" also matches "a", the parent level itself.
			return more == tmore || (more && rest == "#")
		}
		f, topic = rest, trest
	}
}
