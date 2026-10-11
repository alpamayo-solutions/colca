package mqttsrv

import (
	"encoding/json"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	mqtt "github.com/mochi-mqtt/server/v2"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/tokenauth/tokentest"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// A live human session follows the _Group definitions the node holds, not the
// ones it connected under: a definition that drops a grant stops the open
// subscription at its next message, and one that adds it back resumes delivery,
// both without a reconnect or a token renewal.
func TestHumanSessionFollowsGroupDefinitionChanges(t *testing.T) {
	w := newHumanWorld(t)
	eng := w.srv.hook.engine()
	w.ver.SetGroupIndex(eng.Groups())

	const groupTopic = "colca/v1/_Group/n0/01HGRP-VIEWERS"
	define := func(grants ...string) {
		t.Helper()
		payload, err := json.Marshal(map[string]any{"id": "01HGRP-VIEWERS", "name": "viewers", "grants": grants})
		if err != nil {
			t.Fatal(err)
		}
		// The parent hands the definition down, as it does after grantsync wrote it.
		if _, err := eng.IngestDownlinkDefinition(groupTopic, payload, time.Now().UnixMilli()); err != nil {
			t.Fatalf("store _Group definition: %v", err)
		}
	}
	readM1 := "read:" + authtest.ElementID("m1") + "/#"
	define(readM1)

	tok := w.iss.MintOpt(tokentest.MintOpts{
		Sub: "anna", Groups: []string{"01HGRP-VIEWERS"}, Exp: time.Now().Add(5 * time.Minute),
	})
	c, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", "anna", tok)
	if err != nil {
		t.Fatalf("human connect: %v", err)
	}
	defer c.Disconnect(100)

	const filter = "colca/v1/_Metric/+/m1/#"
	const topic = "colca/v1/_Metric/m1/m1/temp"
	msgs := make(chan paho.Message, 8)
	stok := c.Subscribe(filter, 1, func(_ paho.Client, m paho.Message) { msgs <- m })
	if !stok.WaitTimeout(5*time.Second) || stok.Error() != nil {
		t.Fatalf("subscribe: %v", stok.Error())
	}
	if st, ok := stok.(*paho.SubscribeToken); ok && st.Result()[filter] == 0x80 {
		t.Fatal("filter the group grants was denied")
	}
	expect := func(want string) {
		t.Helper()
		select {
		case m := <-msgs:
			if string(m.Payload()) != want {
				t.Fatalf("delivered %s, want %s", m.Payload(), want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s was never delivered", want)
		}
	}
	w.srv.DeliverLocal(topic, []byte(`{"v":1}`), false)
	expect(`{"v":1}`)

	// The group loses the grant. The next value must not reach the session.
	const denyLine = `colca_acl_denials_total{action="sub"}`
	deniedBefore := scrapeMetric(t, w.m, denyLine)
	auditsBefore := w.st.NextOffset("audit")
	define()
	w.srv.DeliverLocal(topic, []byte(`{"v":2}`), false)
	select {
	case m := <-msgs:
		t.Fatalf("delivered %s after the group lost the grant", m.Payload())
	case <-time.After(300 * time.Millisecond):
	}
	if got := scrapeMetric(t, w.m, denyLine); got <= deniedBefore {
		t.Fatalf("%s = %v after a refused delivery, want more than %v", denyLine, got, deniedBefore)
	}
	audits, _, err := w.st.Read("audit", auditsBefore, 100, nil)
	if err != nil || len(audits) == 0 {
		t.Fatalf("no audit record for the refused delivery: records=%d err=%v", len(audits), err)
	}
	var denial map[string]any
	if err := json.Unmarshal(audits[len(audits)-1].Payload, &denial); err != nil {
		t.Fatalf("decode audit: %v", err)
	}
	if denial["outcome"] != "denied" || denial["reason_code"] != "subscribe_denied" {
		t.Fatalf("audit = %v, want a subscribe_denied denial", denial)
	}

	// The grant comes back: delivery resumes on the same connection.
	define(readM1)
	w.srv.DeliverLocal(topic, []byte(`{"v":3}`), false)
	expect(`{"v":3}`)
	if !c.IsConnectionOpen() {
		t.Fatal("the session was disconnected; it should only have been re-scoped")
	}
}

// A resolution that started before a renewal must not overwrite the renewed
// token's grants, and an older generation never replaces a newer one.
func TestHumanSessionReresolveKeepsNewerState(t *testing.T) {
	cl := &mqtt.Client{}
	old := humanSession{client: cl, token: &sessionToken{}, groupsGen: 0}
	renewed := humanSession{client: cl, token: &sessionToken{}, groupsGen: 1}
	h := newHumanSessions()
	h.put("c", renewed)

	stale := &uns.Entry{ULID: "stale"}
	if got, _ := h.reresolved("c", old, stale, 2); got.entry == stale {
		t.Fatal("a resolution of the replaced token overwrote the renewed one")
	}
	fresh := &uns.Entry{ULID: "fresh"}
	if got, _ := h.reresolved("c", renewed, fresh, 2); got.entry != fresh || got.groupsGen != 2 {
		t.Fatalf("resolution at a newer generation not stored: %+v", got)
	}
	if got, _ := h.reresolved("c", renewed, stale, 1); got.entry != fresh {
		t.Fatal("an older generation replaced a newer one")
	}
	if _, ok := h.reresolved("gone", renewed, fresh, 3); ok {
		t.Fatal("a dropped session came back")
	}
}
