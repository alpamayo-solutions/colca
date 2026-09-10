package uns

import (
	"fmt"
	"os"
	"sync/atomic"
)

// DefaultRoot is the first topic segment when a deployment chooses none.
const DefaultRoot = "colca"

// Version is the second topic segment of every topic this package understands.
const Version = "v1"

// RootEnv names the environment variable that sets the root for a process.
const RootEnv = "COLCA_TOPIC_ROOT"

var root atomic.Pointer[string]

// Root is the first topic segment of every record this process handles. All
// nodes of one tree share it: nodes do not translate between roots, so a record
// under another root is refused like any topic outside the namespace.
func Root() string {
	if r := root.Load(); r != nil {
		return *r
	}
	return DefaultRoot
}

// Prefix is how every topic starts: the root and the version with the trailing
// slash, for example "colca/v1/".
func Prefix() string { return Root() + "/" + Version + "/" }

// SetRoot changes the root for the whole process. It is meant to be called at
// start-up, before anything builds or accepts a topic. An invalid root is
// refused and the previous one stays.
func SetRoot(r string) error {
	if err := ValidRoot(r); err != nil {
		return err
	}
	root.Store(&r)
	return nil
}

// SetRootFromEnv sets the root from COLCA_TOPIC_ROOT and keeps the default
// when the variable is unset or empty. Binaries that build topics without a
// node config call it first.
func SetRootFromEnv() error {
	r := os.Getenv(RootEnv)
	if r == "" {
		r = DefaultRoot
	}
	if err := SetRoot(r); err != nil {
		return fmt.Errorf("%s: %w", RootEnv, err)
	}
	return nil
}

// ValidRoot reports why r cannot be a topic root. A root is one topic segment
// of at most 64 letters, digits, '-', '_' or '.', starting with a letter or a
// digit — which rules out MQTT wildcards, the reserved "$" space and the "_"
// that marks a contract.
func ValidRoot(r string) error {
	if r == "" || len(r) > 64 {
		return fmt.Errorf("topic root must be 1 to 64 characters, got %q", r)
	}
	for i, c := range r {
		letterOrDigit := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if letterOrDigit || i > 0 && (c == '-' || c == '_' || c == '.') {
			continue
		}
		return fmt.Errorf("topic root %q: only letters, digits, '-', '_' and '.' are allowed, starting with a letter or digit", r)
	}
	return nil
}
