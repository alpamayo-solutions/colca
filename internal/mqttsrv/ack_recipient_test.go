package mqttsrv

import (
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/authtest"
)

// acksSeen subscribes a person's session to every ack and collects the payloads.
func acksSeen(t *testing.T, c paho.Client) func() []string {
	t.Helper()
	var mu sync.Mutex
	var got []string
	tk := c.Subscribe("colca/v1/_Ack/#", 1, func(_ paho.Client, m paho.Message) {
		mu.Lock()
		got = append(got, string(m.Payload()))
		mu.Unlock()
	})
	if !tk.WaitTimeout(5*time.Second) || tk.Error() != nil {
		t.Fatalf("subscribe to acks: %v", tk.Error())
	}
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), got...)
	}
}

func TestAnAckReachesOnlyThePersonWhoSentTheCommand(t *testing.T) {
	w := newHumanWorld(t)
	future := time.Now().Add(5 * time.Minute)
	grants := []string{"cmd:" + authtest.ElementID("m1") + "/#:param", "read:#"}
	connect := func(name string) paho.Client {
		c, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", name, w.iss.Mint(name, grants, future))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Disconnect(100) })
		return c
	}
	anna, bert := connect("anna"), connect("bert")
	annaSaw, bertSaw := acksSeen(t, anna), acksSeen(t, bert)

	command := `{"correlation_id":"anna-1","expires_at":99999999999999}`
	if tk := anna.Publish("colca/v1/_CmdParam/m1/m1/set-speed", 1, false, command); !tk.WaitTimeout(5 * time.Second) {
		t.Fatal("anna's command: no PUBACK")
	}
	eng := w.srv.hook.engine()
	for _, ack := range []string{
		`{"correlation_id":"anna-1","result_code":200,"message":"ok"}`,
		`{"correlation_id":"nobody-knows","result_code":200,"message":"ok"}`,
	} {
		if _, err := eng.IngestAdmin("colca/v1/_Ack/n1/m1/set-speed", []byte(ack)); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for len(annaSaw()) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if got := annaSaw(); len(got) != 1 || got[0] != `{"correlation_id":"anna-1","result_code":200,"message":"ok"}` {
		t.Fatalf("anna received %v, want only the ack of her command", got)
	}
	if got := bertSaw(); len(got) != 0 {
		t.Fatalf("bert received another person's acks: %v", got)
	}
}
