package uns

import (
	"encoding/json"
	"strings"
)

// ServiceContext is where a service's own records live: its mount, then its
// name. Without the name, services sharing a mount, and every unplaced service,
// would publish _ServiceDetails to one topic and overwrite each other. Python
// services and colca-service both build it, tested against
// vectors/service_context.json.
func ServiceContext(mount, serviceName string) []string {
	context := make([]string, 0, 4)
	for _, part := range strings.Split(mount, "/") {
		if part != "" {
			context = append(context, part)
		}
	}
	return append(context, serviceName)
}

// ServiceRecordAuthor names the identity a retained record about a service was
// authored by, or "" for a contract no service authors about itself. It reads
// the same field the door pins on the way in: a _ServiceDetails payload's "id"
// must equal the authenticated identity that published it.
//
// Retiring an identity's records has to ask each record who wrote it rather
// than compute the topic the identity would publish to today. Where those
// records sit is the service's mount, and a service that moved left one
// standing at every mount it ever had; a computed topic names only the last
// one.
func ServiceRecordAuthor(contract string, payload []byte) string {
	if contract != "_ServiceDetails" || len(payload) == 0 {
		return ""
	}
	var record struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(payload, &record) != nil {
		return ""
	}
	return record.ID
}
