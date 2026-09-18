package node

import (
	"encoding/json"
	"log/slog"
	"net"
	"sort"
	"strings"
	"time"
)

func interfaceType(iface net.Interface) string {
	name := strings.ToLower(iface.Name)
	switch {
	case iface.Flags&net.FlagLoopback != 0:
		return "loopback"
	case strings.HasPrefix(name, "wl") || strings.Contains(name, "wifi") || strings.Contains(name, "wlan"):
		return "wifi"
	case strings.Contains(name, "wwan") || strings.Contains(name, "cell"):
		return "cellular"
	case strings.HasPrefix(name, "br") || strings.Contains(name, "bridge"):
		return "bridge"
	case strings.HasPrefix(name, "tun") || strings.HasPrefix(name, "tap") || strings.Contains(name, "vpn"):
		return "vpn"
	case strings.Contains(name, "docker") || strings.HasPrefix(name, "veth") || strings.HasPrefix(name, "vir"):
		return "virtual"
	case iface.HardwareAddr != nil:
		return "ethernet"
	default:
		return "other"
	}
}

func networkInventory(now time.Time) []map[string]any {
	interfaces, err := net.Interfaces()
	if err != nil {
		// An empty list and a failed enumeration are different facts, so say why the
		// inventory is empty.
		slog.Default().Warn("network inventory unavailable — the node will report no interfaces",
			"err", err)
		return []map[string]any{}
	}
	if len(interfaces) == 0 {
		slog.Default().Warn("network inventory is empty — the host reported no interfaces at all")
	}
	sort.Slice(interfaces, func(i, j int) bool { return interfaces[i].Name < interfaces[j].Name })
	out := make([]map[string]any, 0, len(interfaces))
	for _, iface := range interfaces {
		addresses := []string{}
		if values, err := iface.Addrs(); err == nil {
			for _, address := range values {
				addresses = append(addresses, address.String())
			}
			sort.Strings(addresses)
		}
		out = append(out, map[string]any{
			"name":           iface.Name,
			"interface_type": interfaceType(iface),
			"addresses":      addresses,
			"mac_address":    iface.HardwareAddr.String(),
			"observed_at":    now.Unix(),
		})
	}
	return out
}

func sameNetworkInventory(held any, current []map[string]any) bool {
	heldList, ok := held.([]any)
	if !ok || len(heldList) != len(current) {
		return false
	}
	for i, raw := range heldList {
		heldItem, ok := raw.(map[string]any)
		if !ok {
			return false
		}
		heldCopy := map[string]any{}
		for key, value := range heldItem {
			if key != "observed_at" {
				heldCopy[key] = value
			}
		}
		currentCopy := map[string]any{}
		for key, value := range current[i] {
			if key != "observed_at" {
				currentCopy[key] = value
			}
		}
		heldJSON, heldErr := json.Marshal(heldCopy)
		currentJSON, currentErr := json.Marshal(currentCopy)
		if heldErr != nil || currentErr != nil || string(heldJSON) != string(currentJSON) {
			return false
		}
	}
	return true
}

func nodeHealthMetrics() []map[string]any {
	return []map[string]any{
		{"key": "health", "name": "Service health", "metric": "edge:services_healthy_pct", "description": "Share of services on this node reporting healthy.", "query": `max({{__name__=~"(edge|hub):services_healthy_pct",node_id=~"{node_id_pattern}"}}) * 100`, "visualization": "gauge", "unit": "%", "precision": 1, "min_value": 0, "max_value": 100, "thresholds": map[string]any{"warning": 90, "critical": 50}},
		{"key": "cpu", "name": "CPU", "metric": "cpu_idle_pct", "description": "Node CPU utilization.", "query": `100 - (avg({{__name__=~"(edge|hub):cpu_idle_pct",node_id=~"{node_id_pattern}"}}) * 100)`, "visualization": "timeline", "unit": "%", "precision": 1, "min_value": 0, "max_value": 100, "thresholds": map[string]any{"warning": 70, "critical": 90}},
		{"key": "disk", "name": "Disk", "metric": "disk_used_pct", "description": "Node disk utilization.", "query": `max({{__name__=~"(edge|hub):disk_used_pct",node_id=~"{node_id_pattern}"}}) * 100`, "visualization": "timeline", "unit": "%", "precision": 1, "min_value": 0, "max_value": 100, "thresholds": map[string]any{"warning": 70, "critical": 90}},
		{"key": "replication_lag", "name": "Replication lag", "metric": "replication_lag_records", "description": "Records waiting behind this node's replication cursors.", "query": `max({{__name__=~"(edge|hub):replication_lag_records",node_id=~"{node_id_pattern}"}})`, "visualization": "timeline", "unit": "records", "precision": 0, "min_value": 0, "thresholds": map[string]any{}},
		{"key": "uplink_backlog_age", "name": "Upload backlog age", "metric": "uplink_next_record_age_seconds", "description": "Age of the next retained record awaiting uplink progress. Includes local-only records until progress is acknowledged; zero when caught up.", "query": `max({{__name__=~"(edge|hub):uplink_next_record_age_seconds",node_id=~"{node_id_pattern}",stream="metrics"}}) / 86400`, "visualization": "timeline", "unit": "days", "precision": 1, "min_value": 0, "thresholds": map[string]any{}},
		{"key": "broker_metric_bytes", "name": "Buffered metric storage", "metric": "broker_retained_bytes", "description": "Logical bytes in the broker's metric stream, including uploaded and local-only records still retained. Disk files and historian storage use additional space.", "query": `max({{__name__=~"(edge|hub):broker_retained_bytes",node_id=~"{node_id_pattern}",stream="metrics"}}) / 1048576`, "visualization": "timeline", "unit": "MiB", "precision": 1, "min_value": 0, "thresholds": map[string]any{}},
	}
}
