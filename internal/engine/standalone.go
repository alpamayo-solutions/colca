package engine

import (
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// CommandRetired reports a command accepted before permanent ownership transfer.
// Its stored history remains intact, but neither a new HTTP cursor nor MQTT
// redelivery may turn it into a physical action after the transfer. Receipts
// remain readable, so existing operation outcomes are not lost.
func (e *Engine) CommandRetired(record store.StoredRecord) bool {
	parsed, err := uns.Parse(record.Topic)
	if err != nil || !uns.IsCommand(e.ClassOf(parsed.Contract)) {
		return false
	}
	state, err := e.store.StandaloneGet()
	if err != nil {
		return true // a corrupt/unavailable trust journal cannot authorize replay
	}
	return state != nil && record.Offset < state.CommandsBefore
}
