package store

// pageBytes bounds the stored bytes of the records or KV entries one page
// returns. A page stops before the entry that would take it past max, and the
// caller's next position or token resumes at that entry. A page always holds
// one entry, however large, so a reader moves on whatever a single record
// weighs. max 0 is no bound.
type pageBytes struct{ max, used uint64 }

// take reports whether an entry of size stored bytes still belongs on a page
// that already holds n entries, and counts it if so.
func (p *pageBytes) take(size, n int) bool {
	b := uint64(size) //nolint:gosec // a length is never negative
	if p.max > 0 && n > 0 && p.used+b > p.max {
		return false
	}
	p.used += b
	return true
}
