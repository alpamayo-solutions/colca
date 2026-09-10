package uns

import "strings"

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
