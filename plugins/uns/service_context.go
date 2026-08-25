package uns

import "strings"

// ServiceContext is where a service's own records live: its mount, then its
// name.
//
// The name segment is not decoration. Without it every service sharing a mount
// shares one address — and an UNPLACED service has no mount at all, so on a
// node running several of them (a projector, an audit writer, a UI, a dataops)
// they would all publish their retained _ServiceDetails to the same topic and
// erase each other. Whichever wrote last would be the only service the node
// appeared to have; the rest would lose their projected row, and any catalogue
// naming one (_DataTags.connector) then parks forever waiting for a service
// that never comes back.
//
// Stated here rather than at each caller because two languages need it
// natively — Python services publish their own registration, colca-service
// publishes it for those that cannot — and the two are pinned against one
// golden vector file (colca-data-contracts vectors/service_context.json).
func ServiceContext(mount, serviceName string) []string {
	context := make([]string, 0, 4)
	for _, part := range strings.Split(mount, "/") {
		if part != "" {
			context = append(context, part)
		}
	}
	return append(context, serviceName)
}
