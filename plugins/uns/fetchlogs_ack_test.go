package uns

import "testing"

func TestFetchLogsAckTopic(t *testing.T) {
	for topic, want := range map[string]string{
		"colca/v1/_Ack/n-edge1/site1/edge1/fetchLogs": "site1/edge1/fetchLogs",
		"colca/v1/_Ack/n-edge1/fetchLogs":             "fetchLogs",
		"colca/v1/_Ack/n-edge1/site1/edge1/revoke":    "",
		"colca/v1/_CmdAdmin/n-edge1/fetchLogs":        "",
		"colca/v1/_Log/n-edge1/fetchLogs":             "",
	} {
		path, ok := FetchLogsAck(topic)
		if ok != (want != "") || path != want {
			t.Errorf("FetchLogsAck(%s) = %q, %v; want %q", topic, path, ok, want)
		}
	}
}

func TestMayReadFetchLogsAck(t *testing.T) {
	const ack = "colca/v1/_Ack/n-edge1/site1/edge1/fetchLogs"
	reader := &Entry{ULID: "r", Kind: KindExternal, Grants: []string{"read:#"}}
	admin := &Entry{ULID: "a", Kind: KindExternal, Grants: []string{"cmd:#:admin"}}
	operator := &Entry{ULID: "o", Kind: KindExternal, Grants: []string{"cmd:#:operate"}}
	for _, tc := range []struct {
		name  string
		e     *Entry
		topic string
		actor string
		want  bool
	}{
		{"other acks follow the read grants", reader, "colca/v1/_Ack/n-edge1/site1/edge1/revoke", "x", true},
		{"read grant alone", reader, ack, "x", false},
		{"the requester", reader, ack, "r", true},
		{"admin class", admin, ack, "x", true},
		{"another class", operator, ack, "x", false},
		{"no identity", nil, ack, "", false},
		{"unattributed ack, no admin", reader, ack, "", false},
	} {
		if got := MayReadFetchLogsAck(nil, tc.e, tc.topic, tc.actor); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
