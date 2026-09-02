package node

import (
	"encoding/json"
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
		return []map[string]any{}
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
		copy := map[string]any{}
		for key, value := range heldItem {
			if key != "observed_at" {
				copy[key] = value
			}
		}
		currentCopy := map[string]any{}
		for key, value := range current[i] {
			if key != "observed_at" {
				currentCopy[key] = value
			}
		}
		heldJSON, heldErr := json.Marshal(copy)
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
		{"key": "cpu", "name": "CPU", "metric": "node_cpu_seconds_total", "description": "Node CPU utilization.", "query": `100 - (avg(rate(node_cpu_seconds_total{{mode="idle",node_id=~"{node_id_pattern}"}}[5m])) * 100)`, "visualization": "timeline", "unit": "%", "precision": 1, "min_value": 0, "max_value": 100, "thresholds": map[string]any{"warning": 70, "critical": 90}},
		{"key": "disk", "name": "Disk", "metric": "node_filesystem_avail_bytes", "description": "Node disk utilization.", "query": `max((1 - node_filesystem_avail_bytes{{fstype!~"tmpfs|overlay",mountpoint!~"/boot.*",node_id=~"{node_id_pattern}"}} / node_filesystem_size_bytes{{fstype!~"tmpfs|overlay",mountpoint!~"/boot.*",node_id=~"{node_id_pattern}"}}) * 100)`, "visualization": "timeline", "unit": "%", "precision": 1, "min_value": 0, "max_value": 100, "thresholds": map[string]any{"warning": 70, "critical": 90}},
		{"key": "replication_lag", "name": "Replication lag", "metric": "colca_cursor_lag_records", "description": "Records waiting behind this node's replication cursors.", "query": `max(colca_cursor_lag_records{{node_id=~"{node_id_pattern}"}})`, "visualization": "timeline", "unit": "records", "precision": 0, "min_value": 0, "thresholds": map[string]any{}},
	}
}
