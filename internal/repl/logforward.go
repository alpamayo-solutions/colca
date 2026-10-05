package repl

import (
	"strings"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// logsStream is the lane logForwarding applies to.
const logsStream = "logs"

// logForwarding decides which _Log records the logs lane sends to the parent.
// Every record stays in the local logs stream; this only decides what rises.
// It is a property of this node's link to its parent, so a node that relays
// its children's records applies its own policy to them as well: each hop
// filters with its own settings.
type logForwarding struct {
	minRank  int
	services map[string]int
}

// newLogForwarding builds the policy from a validated parent.logs block.
// Unknown levels cannot reach here (config.Validate rejects them); one that
// does anyway is ignored, falling back to the node-wide minimum.
func newLogForwarding(cfg config.ParentLogs) logForwarding {
	minRank, ok := config.LogLevelRank(cfg.EffectiveMinLevel())
	if !ok {
		minRank, _ = config.LogLevelRank(config.DefaultParentLogMinLevel)
	}
	f := logForwarding{minRank: minRank, services: map[string]int{}}
	for svc, level := range cfg.EffectiveServices() {
		if rank, ok := config.LogLevelRank(level); ok {
			f.services[svc] = rank
		}
	}
	return f
}

// forward reports whether a logs-stream record goes to the parent and, when it
// does not, the level it was withheld at. Only _Log records are filtered: other
// contracts on the logs stream (such as _StreamGap markers) and unparseable
// topics pass, and so does a _Log record whose last segment is not a known
// level, so a malformed record is never hidden from the parent.
func (f logForwarding) forward(topic string) (ok bool, level string) {
	p, err := uns.Parse(topic)
	if err != nil || p.Contract != "_Log" {
		return true, ""
	}
	segs := strings.Split(p.Path, "/")
	level = segs[len(segs)-1]
	rank, known := config.LogLevelRank(level)
	if !known {
		return true, ""
	}
	minRank := f.minRank
	if len(segs) >= 2 {
		if r, ok := f.services[segs[len(segs)-2]]; ok {
			minRank = r
		}
	}
	if rank >= minRank {
		return true, ""
	}
	return false, level
}
