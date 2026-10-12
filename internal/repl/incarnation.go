package repl

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The parent side of the store-incarnation check.
//
// A child remembers offsets of its parent's store: how far it read commands and
// definitions, and the highest offset of each of its own streams the parent
// confirmed. They are valid for one incarnation of that store (see
// store.Store.StoreID). A parent restored from a backup has the id it was
// copied with and positions behind what its children recorded: it would hand
// them nothing until its streams grew past their old heads, and it lacks what
// they sent since the backup without either side noticing.
//
// So a child states what it recorded, and the parent compares. A position this
// store has not reached proves the store regressed; it takes a new incarnation
// id, once, and every child that presents the old id is told to re-sync
// (resync.go). A child from before the handshake states nothing and replicates
// as it always did.

// storeHeader carries the parent's incarnation id on a 409 that tells a child
// to re-sync.
const storeHeader = "Colca-Store"

// childVerdict is what a parent does with a child's request.
type childVerdict int

const (
	// childProceed: the child knows this incarnation, or nothing says otherwise.
	childProceed childVerdict = iota
	// childResync: the child must re-sync; this store's id stands.
	childResync
	// childReincarnate: the child proves this incarnation regressed. The store
	// takes a new id, then the child re-syncs.
	childReincarnate
	// childCannotResync: a child from before the handshake stands past this
	// store's head. It cannot be told; the operator is.
	childCannotResync
)

// judgeChild decides from four facts: this store's incarnation, whether the
// child speaks the handshake, the incarnation it recorded ("" when it never
// recorded one), and whether a position it states is ahead of this store.
func judgeChild(current string, capable bool, recorded string, ahead bool) childVerdict {
	switch {
	case !capable:
		if ahead {
			return childCannotResync
		}
		return childProceed
	case recorded != "" && recorded != current:
		// It knows another incarnation. Whether this one regressed was settled
		// when it took its id.
		return childResync
	case !ahead:
		return childProceed
	case recorded == current:
		return childReincarnate
	default:
		// It never recorded an incarnation, so its positions cannot be held
		// against this one. It still has to move them.
		return childResync
	}
}

// childPosition is what a child states about itself in a request.
type childPosition struct {
	// store is the child's own incarnation.
	store string
	// commands and definitions are the next offsets it reads; 0 when unstated.
	commands, definitions uint64
	// acked is, per stream, the highest of its offsets it was told is stored.
	acked map[string]uint64
}

// parseAcked reads "stream:offset,stream:offset". What does not parse is left
// out: it is evidence, and only what was stated counts.
func parseAcked(v string) map[string]uint64 {
	if v == "" {
		return nil
	}
	out := map[string]uint64{}
	for _, part := range strings.Split(v, ",") {
		stream, offset, ok := strings.Cut(part, ":")
		if !ok || !uns.IsUplinkStream(stream) {
			continue
		}
		if n, err := strconv.ParseUint(offset, 10, 64); err == nil && n > 0 {
			out[stream] = n
		}
	}
	return out
}

func formatAcked(marks map[string]uint64) string {
	parts := make([]string, 0, len(marks))
	for stream, n := range marks {
		if n > 0 {
			parts = append(parts, stream+":"+strconv.FormatUint(n, 10))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// aheadOf says where a child's stated position is ahead of what st holds, ""
// when nowhere. A stream head never moves back and a high-water mark only
// follows what was applied, so on a store that kept its history none of these
// can be true.
func aheadOf(st *store.Store, child string, p childPosition) string {
	if head := st.NextOffset("commands"); p.commands > head {
		return fmt.Sprintf("it read commands up to offset %d, the stream here ends at %d", p.commands-1, head-1)
	}
	if head := st.NextOffset("definitions"); p.definitions > head {
		return fmt.Sprintf("it read definitions up to offset %d, the stream here ends at %d", p.definitions-1, head-1)
	}
	// Marks are kept per child store. A child this node has no store on record
	// for (never replicated, or retired and enrolled again) starts clean, and
	// so does a stream without a mark: neither says anything about this store.
	if p.store == "" || st.ChildStore(child) != p.store {
		return ""
	}
	streams := make([]string, 0, len(p.acked))
	for stream := range p.acked {
		streams = append(streams, stream)
	}
	sort.Strings(streams)
	for _, stream := range streams {
		if hwm := st.HWMGet(child, stream); hwm > 0 && p.acked[stream] > hwm {
			return fmt.Sprintf("it was told its %s records up to offset %d are stored here, this store has them up to %d",
				stream, p.acked[stream], hwm)
		}
	}
	return ""
}

// judge applies judgeChild to one request and carries out the verdict. resync
// is true when the child has to re-sync before anything else is exchanged.
func (s *Server) judge(child string, recorded *string, p childPosition) (resync bool, err error) {
	st := s.eng.Store()
	current := st.StoreID()
	ahead := aheadOf(st, child, p)
	claimed := ""
	if recorded != nil {
		claimed = *recorded
	}
	verdict := judgeChild(current, recorded != nil, claimed, ahead != "")
	if verdict == childReincarnate && !st.MayHaveRegressed() {
		// This process minted the incarnation the child names, and its history
		// only grew since. The child is wrong about it, whatever the reason;
		// it re-syncs alone and the id stands for everyone else.
		s.log.Warn("a child states a position ahead of a store incarnation that began in this run — "+
			"the incarnation stands and the child re-syncs", "child", child, "evidence", ahead, "store", current)
		verdict = childResync
	}
	switch verdict {
	case childProceed:
		if wasAhead, _, _ := s.regressed.Recovered(child, time.Now()); wasAhead {
			s.log.Info("the child is no longer ahead of this store", "child", child)
		}
		return false, nil
	case childCannotResync:
		s.metrics.ChildAheadOfStore()
		if report, _, since := s.regressed.Failed(child, time.Now()); report {
			s.log.Error("a child is ahead of this store and cannot re-sync — this store was restored from a backup "+
				"or replaced, and the child's colca predates the re-sync; it receives no commands until the "+
				"stream here passes its position, and what it sent since the backup is not sent again. Upgrade the child",
				"child", child, "evidence", ahead, "for", since.Round(time.Second))
		}
		return false, nil
	case childReincarnate:
		id, changed, err := st.ReincarnateFrom(current, "child "+child+": "+ahead)
		if err != nil {
			return true, err
		}
		if changed {
			s.metrics.StoreReincarnated("regression")
			s.log.Error("this store is behind what a child recorded of it — it was restored from a backup or "+
				"replaced. It takes a new incarnation; its children re-sync: they take commands from where "+
				"this store stands and send again what it lacks",
				"child", child, "evidence", ahead, "previous_store", current, "store", id)
		}
		return true, nil
	default:
		s.log.Info("a child re-syncs with this store", "child", child,
			"child_knows_store", claimed, "store", current, "evidence", ahead)
		return true, nil
	}
}

// refuseForResync answers 409 when the child has to re-sync first, and reports
// whether it answered.
func (s *Server) refuseForResync(w http.ResponseWriter, child string, recorded *string, p childPosition) bool {
	resync, err := s.judge(child, recorded, p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return true
	}
	if !resync {
		return false
	}
	w.Header().Set(storeHeader, s.eng.Store().StoreID())
	http.Error(w, "store changed: re-sync with this node's store before replicating", http.StatusConflict)
	return true
}

// addHandshake adds what a child needs to place itself against this store: its
// incarnation, whether the child has to re-sync, where it resumes on commands
// and definitions, and the highest offset of each stream held from it.
func (s *Server) addHandshake(resp map[string]any, child string, resync bool) {
	st := s.eng.Store()
	resp["store"] = st.StoreID()
	resp["resync"] = resync
	// Where this store last saw the child ask from: everything before it was
	// handed over while this history was the child's too.
	resume, _ := st.CursorLookup(uns.DownlinkCursorPrefix+child, "commands")
	resp["resume"] = max(resume, 1)
	defResume, _ := st.CursorLookup(uns.DownlinkDefCursorPrefix+child, "definitions")
	resp["def_resume"] = max(defResume, 1)
	hwms := map[string]uint64{}
	for _, stream := range uplinkStreams() {
		hwms[stream] = st.HWMGet(child, stream)
	}
	resp["hwms"] = hwms
}

// adoptChildStore records the incarnation a child replicates from. A child
// whose store was rebuilt under the same identity restarts at offset 1, and one
// whose store regressed hands out offsets again; the marks kept for the old
// incarnation would drop what it sends and report it delivered. Adopting the
// new one clears them first.
func (s *Server) adoptChildStore(child, id string) error {
	reset, err := s.eng.Store().AdoptChildStore(child, id)
	if err != nil {
		return err
	}
	if reset {
		s.log.Warn("the child's store has a new incarnation: its replication marks were reset",
			"child", child, "store", id)
	}
	return nil
}
