// Commands no service executes.
//
// A command for a service is stored and left to its executor, which answers with
// an _Ack. A command no service executes would get no answer, and its sender
// waited out the whole lifetime. Services therefore announce the commands they
// execute in their _ServiceDetails record ("commands": [{"contract", "path"}]),
// and the node answers a command nobody announced with a 404 _Ack instead of
// storing it, when some service announced a command at the same element. That
// catches a removed verb, a misspelt or wrong-case verb, and a verb sent to the
// wrong element that takes others. Commands at elements where nobody announces
// anything still pass through untouched: their executor may simply not announce
// (a machine, or a service built before announcements).
//
// A payload the node refuses (a command that is not an object, NaN or Infinity)
// is answered with a 400 _Ack when a correlation id can be read from it.

package engine

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// commandRoute is one command a service announced: a contract and a node-local
// path, which may use + for one segment and a trailing # for the rest.
type commandRoute struct {
	Contract string `json:"contract"`
	Path     string `json:"path"`
}

// announcedRoutes reads every command the local services announce. A service
// that is down still counts: its commands wait in the stream for it.
func (e *Engine) announcedRoutes() ([]commandRoute, error) {
	var routes []commandRoute
	after := ""
	for {
		entries, next, err := e.store.KVScanPage("", after, 1000, []string{"_ServiceDetails"})
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.NodeID != e.cfg.ULID {
				continue
			}
			var details struct {
				Commands []commandRoute `json:"commands"`
			}
			if json.Unmarshal(entry.Payload, &details) != nil {
				continue
			}
			for _, route := range details.Commands {
				if route.Contract != "" && route.Path != "" {
					routes = append(routes, route)
				}
			}
		}
		if next == "" {
			return routes, nil
		}
		after = next
	}
}

// unannounced says whether a command to this node goes unanswered by any
// service, and if so the 404 message. Commands the node executes itself, and
// commands at an element where nobody announced anything, are not its concern.
func (e *Engine) unannounced(p uns.Parsed) (string, bool) {
	if p.NodeID != e.cfg.ULID || (e.exec != nil && e.exec.Handles(p.Contract)) {
		return "", false
	}
	routes, err := e.announcedRoutes()
	if err != nil {
		e.log.Warn("command routes unreadable; command passed on", "err", err)
		return "", false
	}
	element := parentPath(p.Path)
	var siblings []string
	claimed := false
	for _, route := range routes {
		if route.Contract == p.Contract && routeMatches(route.Path, p.Path) {
			return "", false
		}
		if strings.HasSuffix(route.Path, "#") || !routeMatches(parentPath(route.Path), element) {
			continue
		}
		claimed = true
		verb := route.Path[strings.LastIndexByte(route.Path, '/')+1:]
		if route.Contract != p.Contract {
			verb += " (" + route.Contract + ")"
		}
		siblings = append(siblings, verb)
	}
	if !claimed {
		return "", false
	}
	sort.Strings(siblings)
	return fmt.Sprintf("no service executes %s %s; %s takes %s",
		p.Contract, p.Path, orRoot(element), strings.Join(dedupe(siblings), ", ")), true
}

// answer settles a command the node refuses to pass on: the sender gets an _Ack
// with code and message, nothing is stored, and a repeat of the same
// correlation id gets the same answer. Without a readable correlation id there
// is no one to answer.
func (e *Engine) answer(p uns.Parsed, payload []byte, actor string, code int, message string) *CommandOutcome {
	id := lenientCorrelationID(payload)
	if id == "" {
		return nil
	}
	outcome := &CommandOutcome{CorrelationID: id, ResultCode: code, Message: message}
	ack, err := json.Marshal(outcome)
	if err != nil {
		return nil
	}
	topic := uns.Prefix() + "_Ack/" + p.NodeID + "/" + p.Path
	verdict, heldTopic, held := e.ledger.admit(id, actor)
	switch verdict {
	case ledgerForeignUse:
		// Another sender's id: that sender keeps it, this one learns nothing new.
		return nil
	case ledgerRepeat:
		if held == nil {
			return nil
		}
		topic, ack = heldTopic, held
		_ = json.Unmarshal(held, outcome)
	default:
		e.ledger.acked(id, topic, ack)
	}
	e.metrics.NodeCmd(p.Contract, p.Path[strings.LastIndexByte(p.Path, '/')+1:], fmt.Sprintf("refused_%d", code))
	e.log.Info("command answered by the node", "contract", p.Contract, "path", p.Path,
		"correlation_id", id, "result_code", outcome.ResultCode, "message", outcome.Message)
	if e.deliver != nil {
		e.deliver(topic, ack, false)
	}
	return outcome
}

// refuseCommandPayload answers a command whose payload the node refused.
func (e *Engine) refuseCommandPayload(p uns.Parsed, payload []byte, actor string, cause error) {
	e.answer(p, payload, actor, 400, "command refused: "+cause.Error())
}

// answeredUnannounced is the Result of a command answered with 404: nothing
// stored, the outcome returned to a caller that waits for it.
func (e *Engine) answeredUnannounced(p uns.Parsed, payload []byte, actor, message string) Result {
	outcome := e.answer(p, payload, actor, 404, message)
	if outcome == nil {
		outcome = &CommandOutcome{ResultCode: 404, Message: message}
	}
	return Result{Answered: true, Command: outcome}
}

var correlationIDPattern = regexp.MustCompile(`"correlation_id"\s*:\s*"((?:[^"\\]|\\.){1,256})"`)

// lenientCorrelationID reads the correlation id from a payload that may not be
// valid JSON as a whole, such as one carrying NaN: the id is still what the
// sender waits on.
func lenientCorrelationID(payload []byte) string {
	if id := correlationID(payload); id != "" {
		return id
	}
	match := correlationIDPattern.FindSubmatchIndex(payload)
	if match == nil {
		return ""
	}
	// The string literal as the sender wrote it, its own quotes included.
	var id string
	if json.Unmarshal(payload[match[2]-1:match[3]+1], &id) != nil {
		return ""
	}
	return id
}

// routeMatches matches a node-local path against an announced one: + stands
// for one segment, a trailing # for the rest (at least the element itself).
func routeMatches(pattern, path string) bool {
	want := strings.Split(pattern, "/")
	got := strings.Split(path, "/")
	for i, segment := range want {
		if segment == "#" && i == len(want)-1 {
			return len(got) >= i
		}
		if i >= len(got) || (segment != "+" && segment != got[i]) {
			return false
		}
	}
	return len(got) == len(want)
}

func parentPath(path string) string {
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		return path[:i]
	}
	return ""
}

func orRoot(element string) string {
	if element == "" {
		return "the node root"
	}
	return element
}

func dedupe(sorted []string) []string {
	out := sorted[:0]
	for i, s := range sorted {
		if i == 0 || s != sorted[i-1] {
			out = append(out, s)
		}
	}
	return out
}
