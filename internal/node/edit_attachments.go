package node

import (
	"errors"

	"github.com/alpamayo-solutions/colca/internal/registry"
)

// editAttachmentWriter keeps registry-owned lifecycle mutations behind
// the narrow port exposed by the UNS Edit executor. The plugin remains
// independent of core packages while every attachment update still goes
// through registry.Manager's validation and durable write path.
type editAttachmentWriter struct {
	registry *registry.Manager
}

func (w editAttachmentWriter) RemountNode(entryJSON []byte) (uint64, int, string) {
	_, offset, err := w.registry.Enroll(entryJSON)
	if err == nil {
		return offset, 200, "node attachment remounted"
	}
	switch {
	case errors.Is(err, registry.ErrConflict):
		return 0, 409, err.Error()
	case errors.Is(err, registry.ErrUnknownElement):
		return 0, 422, err.Error()
	default:
		return 0, 500, err.Error()
	}
}

func (w editAttachmentWriter) DrainNode(ulid string) (uint64, int, string) {
	offset, err := w.registry.Drain(ulid)
	if err == nil {
		return offset, 200, "node attachment draining"
	}
	switch {
	case errors.Is(err, registry.ErrNotEnrolled),
		errors.Is(err, registry.ErrAlreadyDraining),
		errors.Is(err, registry.ErrConflict):
		return 0, 409, err.Error()
	case errors.Is(err, registry.ErrNotNode), errors.Is(err, registry.ErrUnknownElement):
		return 0, 422, err.Error()
	default:
		return 0, 500, err.Error()
	}
}
