// Package httpserver creates Colca HTTP servers with the availability limits
// that are safe for every door. Endpoint-specific body and response limits stay
// with the handlers that understand those payloads.
package httpserver

import (
	"net/http"
	"time"
)

const (
	// ReadHeaderTimeout bounds slowloris-style clients without imposing a
	// deadline on legitimate replication bodies or long-poll responses.
	ReadHeaderTimeout = 5 * time.Second
	// IdleTimeout releases keep-alive connections that are no longer active.
	IdleTimeout = 60 * time.Second
	// MaxHeaderBytes is generous for bearer tokens while bounding attacker-
	// controlled header memory.
	MaxHeaderBytes = 64 << 10
)

// New returns a server with Colca's non-negotiable cross-door limits.
func New(handler http.Handler) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: ReadHeaderTimeout,
		IdleTimeout:       IdleTimeout,
		MaxHeaderBytes:    MaxHeaderBytes,
	}
}

// NewAt is New with an address for callers that use ListenAndServe.
func NewAt(addr string, handler http.Handler) *http.Server {
	server := New(handler)
	server.Addr = addr
	return server
}
