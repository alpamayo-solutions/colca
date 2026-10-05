package mqttsrv

import (
	"strings"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/internal/contracts/contractstest"
)

// A _Log record the log gate collapses gets a successful PUBACK, as any
// accepted publish does, and mochi does not fan the raw publish out: the
// subscriber sees the one stored record only.
func TestACollapsedLogRecordIsAckedAndNotFannedOut(t *testing.T) {
	tbl, err := contracts.Load(contractstest.GeneratedBundlePath(t), "")
	if err != nil {
		t.Fatal(err)
	}
	w := newWorld(t)
	w.eng.SetContracts(tbl)

	sub := connect(t, w.srv.Addr(), "observer-sub", w.obs)
	msgs := make(chan paho.Message, 32)
	if tok := sub.Subscribe("colca/v1/_Log/#", 1, func(_ paho.Client, m paho.Message) { msgs <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	pub := connect(t, w.srv.Addr(), "m1-pub", w.m1)
	line := []byte(`{"timestamp":"2026-10-05T12:00:00Z","level":"ERROR","message":"connection refused",` +
		`"logger_name":"driver","module":"driver","function":"connect","line_no":7}`)
	for i := range 3 {
		tok := pub.Publish("colca/v1/_Log/n1/m1/ERROR", 1, false, line)
		if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("publish %d: no successful PUBACK: %v", i, tok.Error())
		}
	}
	var got []string
	deadline := time.After(1500 * time.Millisecond)
drain:
	for {
		select {
		case m := <-msgs:
			if strings.HasSuffix(m.Topic(), "/m1/ERROR") {
				got = append(got, string(m.Payload()))
			}
		case <-deadline:
			break drain
		}
	}
	if len(got) != 1 {
		t.Fatalf("subscriber saw %d records, want the one stored", len(got))
	}
	if next := w.st.NextOffset("logs"); next != 2 {
		t.Fatalf("logs next offset = %d, want 2: one record stored", next)
	}
}
