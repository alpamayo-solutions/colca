package engine

import (
	"time"

	"github.com/alpamayo-solutions/colca/internal/loggate"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// LogGateAuthor is who a drop notice is attributed to: the node itself, as
// its own log records are (internal/nodelog).
const LogGateAuthor = "colca"

func newLogGate(cfg loggate.Config) *loggate.Gate[Attribution] {
	return loggate.New(cfg, Attribution{
		WrittenBy: LogGateAuthor, ActorID: LogGateAuthor,
		ActorLabel: LogGateAuthor, ActorKind: "system",
	})
}

// gateLog runs a _Log record written on this node through the log gate. held
// is true when the record is not to be stored: it was accepted and is counted
// in its window's summary or drop notice, and res says which. Every other
// record, and every record replicated from a child (gated at that child), is
// left alone.
//
// The publisher sees success for a held record: an MQTT PUBACK and HTTP 202.
// It was accepted; storing it again would only repeat what the summary says.
func (e *Engine) gateLog(class uns.Class, p uns.Parsed, topic string, payload []byte, attribution Attribution) (res Result, held bool) {
	if class != uns.ClassLog || p.NodeID != e.cfg.ULID || !e.logs.Enabled() {
		return Result{}, false
	}
	verdict, untracked, writes := e.logs.Admit(e.clk.Now(), topic, payload, attribution)
	// A window this record ended is summarized before the record that opens
	// the next one.
	e.writeLogGate(writes)
	switch untracked {
	case loggate.UntrackedRepeat:
		e.metrics.LogUntracked(metrics.LogTableRepeats)
	case loggate.UntrackedService:
		e.metrics.LogUntracked(metrics.LogTableServices)
	}
	switch verdict {
	case loggate.Collapsed:
		e.metrics.LogWithheld(metrics.LogCollapsed)
	case loggate.RateLimited:
		e.metrics.LogWithheld(metrics.LogRateLimited)
	default:
		return Result{}, false
	}
	return Result{Withheld: verdict.String(), Stream: uns.StreamFor(class), Topic: topic}, true
}

// writeLogGate stores the gate's summaries and drop notices. They bypass the
// gate: a summary is never collapsed and a notice never spends a budget.
// A failure is counted and logged once per write, never per withheld record.
func (e *Engine) writeLogGate(writes []loggate.Write[Attribution]) {
	for _, w := range writes {
		p, err := uns.Parse(w.Topic)
		if err == nil {
			_, err = e.persistAttributed(uns.ClassLog, p, w.Topic, w.Payload, w.Author)
		}
		if err != nil {
			e.metrics.LogGateWriteFailed(w.Notice)
			e.log.Warn("log gate: summary not written", "notice", w.Notice, "err", err)
		}
	}
}

// RunLogGate ends the gate's windows on time: at each window end it writes the
// collapse summaries and drop notices due. It returns when stop closes;
// FlushLogs then writes what is still pending.
func (e *Engine) RunLogGate(stop <-chan struct{}) {
	if !e.logs.Enabled() {
		return
	}
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		wait := time.Hour // nothing pending: sleep until woken
		if at, ok := e.logs.NextDeadline(); ok {
			wait = max(at.Sub(e.clk.Now()), 0)
		}
		timer.Reset(wait)
		select {
		case <-stop:
			return
		case <-e.logs.Wake():
		case <-timer.C:
		}
		e.writeLogGate(e.logs.Due(e.clk.Now()))
	}
}

// FlushLogs writes every pending summary and drop notice and opens the gate
// for good. A node calls it on Stop, before the store closes; records
// admitted afterwards are stored as they come.
func (e *Engine) FlushLogs() {
	if e.logs.Enabled() {
		e.writeLogGate(e.logs.Close())
	}
}
