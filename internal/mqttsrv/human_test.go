package mqttsrv

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	pahov5 "github.com/eclipse/paho.golang/paho"
	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/internal/tokenauth"
	"github.com/alpamayo-solutions/colca/internal/tokenauth/tokentest"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// humanWorld is the human-door fixture: machine listener + BOTH human doors,
// a fake issuer, one enrolled machine (m1, mount "m1") for cross-checks.
type humanWorld struct {
	srv *Server
	st  *store.Store
	reg *registry.Manager
	iss *tokentest.Issuer
	ver *tokenauth.Verifier
	m   *metrics.Metrics
	m1  *authtest.Machine
}

func newHumanWorld(t *testing.T) *humanWorld {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeID, err := identity.Generate(filepath.Join(t.TempDir(), "n1.key"))
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.New(st, "n1")
	if err != nil {
		t.Fatal(err)
	}
	iss := tokentest.NewIssuer(t)
	m := metrics.New(st, config.Retention{}, nil)
	ver, err := tokenauth.New(tokenauth.Config{
		Issuer: iss.Iss(), Audience: iss.Aud(), JWKSURL: iss.JWKSURL(),
	}, st, m)
	if err != nil {
		t.Fatal(err)
	}
	verifierPrime(t, ver)

	w := &humanWorld{st: st, reg: reg, iss: iss, ver: ver, m: m, m1: authtest.NewMachine(t, "m1")}

	cfg := &config.Config{ULID: "n1", DataDir: t.TempDir(), KeyFile: "unused",
		MQTT:      config.Endpoint{Addr: "127.0.0.1:0"},
		MQTTHuman: config.MQTTHuman{TCPAddr: "127.0.0.1:0", WSAddr: "127.0.0.1:0"},
		Auth:      &config.Auth{Issuer: iss.Iss(), Audience: iss.Aud(), JWKSURL: iss.JWKSURL()},
	}
	s, err := New(cfg, nodeID, reg, ver, nil, m, config.Limits{}.EffectiveMaxRecordBytes())
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	eng := engine.New(st, cfg, reg, s.DeliverLocal, m, nil)
	s.SetEngine(eng)
	// Placement resolves through the engine's element index, so the element m1
	// binds to is authored before it enrolls (id-grants design §4).
	reg.SetNamespace(eng.Elements())
	authtest.EnrollAt(t, reg, eng, w.m1, "m1")
	reg.SetKick(s.Kick)
	t.Cleanup(func() {
		s.Close()
		st.Close()
	})
	w.srv = s
	go func() { _ = s.Serve() }()
	return w
}

// verifierPrime forces one JWKS fetch (Run's first tick without the loop).
func verifierPrime(t *testing.T, v *tokenauth.Verifier) {
	t.Helper()
	done := make(chan struct{})
	stop := make(chan struct{})
	go func() { v.Run(stop); close(done) }()
	// Run fetches immediately; give it a moment then stop the loop.
	time.Sleep(50 * time.Millisecond)
	close(stop)
	<-done
}

// insecureTLS: no client cert (the human doors don't ask for one), server
// cert unverified — pinning model, the NODE pins nothing about humans.
func insecureTLS() *tls.Config {
	return &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13} // #nosec G402 -- test
}

// humanConnect dials a human door with a JWT. scheme "ssl" (tcp) or "wss".
func humanConnect(t *testing.T, addr, scheme, username, token string) (paho.Client, error) {
	t.Helper()
	url := scheme + "://" + addr
	if scheme == "wss" {
		url += "/"
	}
	opts := paho.NewClientOptions().
		AddBroker(url).
		SetTLSConfig(insecureTLS()).
		SetClientID(fmt.Sprintf("h-%s-%d", username, time.Now().UnixNano())).
		SetUsername(username).
		SetPassword(token).
		SetProtocolVersion(4).
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(false)
	c := paho.NewClient(opts)
	tk := c.Connect()
	if !tk.WaitTimeout(5 * time.Second) {
		return c, fmt.Errorf("connect timed out")
	}
	return c, tk.Error()
}

func installPersonalAccessToken(
	t *testing.T, w *humanWorld, id string, scopes []string, expiresAt time.Time,
) (string, string) {
	t.Helper()
	token := "pk_pat_" + id + "_secret"
	digest := sha256.Sum256([]byte(token))
	payload, err := json.Marshal(uns.PersonalAccessToken{
		ID: id, HashedSecret: hex.EncodeToString(digest[:]), OwnerSub: "pat-user",
		OwnerEmail: "pat@example.com", Scopes: scopes,
		Grants:    []string{"read:" + authtest.ElementID("m1") + "/#"},
		ExpiresAt: expiresAt.UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatal(err)
	}
	topic := "colca/v1/" + uns.PersonalAccessTokenContract + "/n1/" + id
	if _, err := w.srv.hook.engine().EntityStore().PublishBatch([]uns.StateRecord{{
		Topic: topic, Payload: payload,
	}}); err != nil {
		t.Fatalf("publish PAT definition: %v", err)
	}
	w.ver.SetPersonalAccessTokenIndex(uns.NewPersonalAccessTokenIndex(
		w.srv.hook.engine().EntityStore(),
	))
	return token, topic
}

func waitHumanConnectionClosed(t *testing.T, client paho.Client) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for client.IsConnectionOpen() {
		if time.Now().After(deadline) {
			t.Fatal("human session was not kicked")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestHumanDoorsConnectAndScope(t *testing.T) {
	w := newHumanWorld(t)
	future := time.Now().Add(5 * time.Minute)
	tok := w.iss.Mint("anna", []string{"read:" + authtest.ElementID("m1") + "/#"}, future)

	for name, dial := range map[string]struct{ addr, scheme string }{
		"tcp": {w.srv.HumanTCPAddr(), "ssl"},
		"ws":  {w.srv.HumanWSAddr(), "wss"},
	} {
		t.Run(name, func(t *testing.T) {
			c, err := humanConnect(t, dial.addr, dial.scheme, "anna", tok)
			if err != nil {
				t.Fatalf("human connect over %s: %v", name, err)
			}
			defer c.Disconnect(100)

			// Granted zone subscribes fine and receives live data.
			msgs := make(chan paho.Message, 8)
			stok := c.Subscribe("colca/v1/_Metric/+/m1/#", 1, func(_ paho.Client, m paho.Message) { msgs <- m })
			if !stok.WaitTimeout(5*time.Second) || stok.Error() != nil {
				t.Fatalf("granted subscribe: %v", stok.Error())
			}
			if st, ok := stok.(*paho.SubscribeToken); ok {
				if qos, found := st.Result()["colca/v1/_Metric/+/m1/#"]; found && qos == 0x80 {
					t.Fatal("granted filter denied")
				}
			}
			w.srv.DeliverLocal("colca/v1/_Metric/m1/m1/temp", []byte(`{"v":1}`), false)
			select {
			case m := <-msgs:
				if m.Topic() != "colca/v1/_Metric/m1/m1/temp" {
					t.Fatalf("topic %s", m.Topic())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("granted data never arrived")
			}

			// Out-of-scope filter → SUBACK 0x80.
			dtok := c.Subscribe("colca/v1/+/+/other/#", 1, func(paho.Client, paho.Message) {})
			dtok.WaitTimeout(3 * time.Second)
			if st, ok := dtok.(*paho.SubscribeToken); ok {
				if qos, found := st.Result()["colca/v1/+/+/other/#"]; !found || qos != 0x80 {
					t.Fatalf("out-of-scope filter must be denied, qos %v", qos)
				}
			}
			audits, _, err := w.st.Read("audit", 1, 100, nil)
			if err != nil || len(audits) == 0 {
				t.Fatalf("read subscribe denial audit: records=%d err=%v", len(audits), err)
			}
			var denial map[string]any
			if err := json.Unmarshal(audits[len(audits)-1].Payload, &denial); err != nil {
				t.Fatalf("decode subscribe denial audit: %v", err)
			}
			if denial["entity_type"] != "mqtt-subscription" || denial["entity_id"] != "other" {
				t.Fatalf("subscribe denial target = %v/%v, want mqtt-subscription/other",
					denial["entity_type"], denial["entity_id"])
			}
		})
	}
}

func TestHumanDoorRejections(t *testing.T) {
	w := newHumanWorld(t)
	future := time.Now().Add(5 * time.Minute)

	cases := []struct {
		name, user, tok string
	}{
		{"garbage token", "anna", "not-a-jwt"},
		{"expired token", "anna", w.iss.Mint("anna", nil, time.Now().Add(-3*time.Minute))},
		{"wrong audience", "anna", w.iss.MintOpt(tokentest.MintOpts{Sub: "anna", Exp: future, Aud: "other"})},
		{"username != sub", "not-anna", w.iss.Mint("anna", nil, future)},
		// OnPublish trusts that no session which reached it can carry an empty
		// identity (mqttsrv.go: `if ident == ""` lets the packet through
		// UNVALIDATED). Half of that trust, for the human door, is that a
		// token whose own "sub" claim is empty is refused at the source
		// (plugins/uns.TokenEntry: "token entry: empty sub") before
		// user == v.Sub is ever reached — so v.Sub can never itself be "". A
		// non-empty username here isolates that: it proves the empty-sub
		// token is rejected on its own terms, not merely because it also
		// happens to mismatch the username.
		{"empty sub claim", "someone", w.iss.MintOpt(tokentest.MintOpts{Sub: "", Exp: future})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cl, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", c.user, c.tok)
			defer cl.Disconnect(50)
			if err == nil {
				t.Fatalf("%s: connect must be refused", c.name)
			}
		})
	}

	// A machine's cert-less token attempt at the MACHINE door still fails
	// (regression pin: the machine door does not accept tokens).
	cl, err := humanConnect(t, w.srv.Addr(), "ssl", "anna", w.iss.Mint("anna", nil, future))
	defer cl.Disconnect(50)
	if err == nil {
		t.Fatal("machine door accepted a token")
	}
}

// A CONNECT with an empty username but a well-formed, correctly signed token
// (real, non-empty sub) is the other half of the OnPublish trust that
// TestHumanDoorRejections' "empty sub claim" case does not reach: it proves
// user == v.Sub itself refuses "" against a real sub, not merely that a
// malformed/empty-sub token gets rejected first.
//
// This needs the MQTT 5 client (paho.golang), not humanConnect's MQTT 3.1.1
// one: paho.mqtt.golang's CONNECT builder refuses to set PasswordFlag unless
// Username is non-empty ("mustn't have password without user as well" —
// message.go), so it cannot even construct the packet this test needs to
// send — the adversarial CONNECT (empty username, password present) would
// silently degrade into an empty-token CONNECT and prove nothing. paho.golang
// exposes UsernameFlag/PasswordFlag independently of the string values, so it
// can send exactly that packet.
func TestHumanDoorRejectsEmptyUsernameAgainstARealToken(t *testing.T) {
	w := newHumanWorld(t)
	tok := w.iss.Mint("anna", nil, time.Now().Add(5*time.Minute))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := tls.Dial("tcp", w.srv.HumanTCPAddr(), insecureTLS())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	c := pahov5.NewClient(pahov5.ClientConfig{Conn: conn})
	_, err = c.Connect(ctx, &pahov5.Connect{
		ClientID:     fmt.Sprintf("h-empty-user-%d", time.Now().UnixNano()),
		UsernameFlag: true,
		Username:     "",
		PasswordFlag: true,
		Password:     []byte(tok),
		KeepAlive:    30,
		CleanStart:   true,
		Properties:   &pahov5.ConnectProperties{},
	})
	if err == nil {
		t.Fatal("connect with an empty username against a valid, non-empty-sub token succeeded, want rejection")
	}
}

func TestHumanPATRevocationAndScopeRemovalKickLiveSessions(t *testing.T) {
	w := newHumanWorld(t)
	expires := time.Now().Add(time.Hour)

	revokedToken, revokedTopic := installPersonalAccessToken(
		t, w, "01M0ZPAT000000000000000101", []string{"broker-mqtt"}, expires,
	)
	revoked, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", "pat-user", revokedToken)
	if err != nil {
		t.Fatalf("connect revoked candidate: %v", err)
	}
	defer revoked.Disconnect(100)

	downscopedToken, _ := installPersonalAccessToken(
		t, w, "01M0ZPAT000000000000000102", []string{"broker-mqtt"}, expires,
	)
	downscoped, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", "pat-user", downscopedToken)
	if err != nil {
		t.Fatalf("connect downscope candidate: %v", err)
	}
	defer downscoped.Disconnect(100)

	if _, err := w.srv.hook.engine().EntityStore().PublishBatch([]uns.StateRecord{{
		Topic: revokedTopic,
	}}); err != nil {
		t.Fatalf("publish PAT tombstone: %v", err)
	}
	installPersonalAccessToken(
		t, w, "01M0ZPAT000000000000000102", []string{"api"}, expires,
	)

	w.srv.sweepInvalidHumanSessions(time.Now())
	waitHumanConnectionClosed(t, revoked)
	waitHumanConnectionClosed(t, downscoped)

	if _, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", "pat-user", revokedToken); err == nil {
		t.Fatal("revoked PAT reconnected")
	}
	if _, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", "pat-user", downscopedToken); err == nil {
		t.Fatal("PAT without broker-mqtt scope reconnected")
	}
}

// The session lives exactly as long as the token: per-delivery denial after
// exp, sweeper kick, fresh-token reconnect works (§5.1).
func TestHumanExpiryKick(t *testing.T) {
	w := newHumanWorld(t)
	shortTok := w.iss.Mint("anna", []string{"read:" + authtest.ElementID("m1") + "/#"}, time.Now().Add(2*time.Second))

	c, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", "anna", shortTok)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Disconnect(100)

	msgs := make(chan paho.Message, 8)
	if tk := c.Subscribe("colca/v1/_Metric/+/m1/#", 1, func(_ paho.Client, m paho.Message) { msgs <- m }); !tk.WaitTimeout(5*time.Second) || tk.Error() != nil {
		t.Fatalf("subscribe: %v", tk.Error())
	}
	w.srv.DeliverLocal("colca/v1/_Metric/m1/m1/temp", []byte(`{"v":1}`), false)
	select {
	case <-msgs:
	case <-time.After(5 * time.Second):
		t.Fatal("pre-expiry delivery never arrived")
	}

	// Wait past exp: per-delivery ACL must now drop deliveries even before
	// the kick lands.
	time.Sleep(2500 * time.Millisecond)
	w.srv.DeliverLocal("colca/v1/_Metric/m1/m1/temp", []byte(`{"v":2}`), false)
	select {
	case m := <-msgs:
		t.Fatalf("post-expiry delivery leaked: %s %s", m.Topic(), m.Payload())
	case <-time.After(700 * time.Millisecond):
	}

	// The sweeper kicks the session (drive it directly — the 10s ticker is
	// wall-clock; the sweep body is the contract).
	w.srv.sweepInvalidHumanSessions(time.Now())
	waitHumanConnectionClosed(t, c)

	// A fresh token reconnects fine.
	c2, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", "anna",
		w.iss.Mint("anna", []string{"read:" + authtest.ElementID("m1") + "/#"}, time.Now().Add(5*time.Minute)))
	if err != nil {
		t.Fatalf("fresh-token reconnect: %v", err)
	}
	c2.Disconnect(100)
}

// Humans publish commands through IngestHuman; writes are rejected (§5.2).
func TestHumanPublishThroughDoor(t *testing.T) {
	w := newHumanWorld(t)
	future := time.Now().Add(5 * time.Minute)
	tok := w.iss.Mint("hmi-user", []string{"cmd:" + authtest.ElementID("m1") + "/#:param", "read:" + authtest.ElementID("m1") + "/#"}, future)

	c, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", "hmi-user", tok)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(100)

	payload := `{"correlation_id":"h1","expires_at":99999999999999}`
	if tk := c.Publish("colca/v1/_CmdParam/m1/m1/set-speed", 1, false, payload); !tk.WaitTimeout(5 * time.Second) {
		t.Fatal("granted human command: no PUBACK")
	}
	recs, _, _ := w.st.Read("commands", 1, 10, nil)
	if len(recs) != 1 || recs[0].Topic != "colca/v1/_CmdParam/m1/m1/set-speed" {
		t.Fatalf("command not persisted: %+v", recs)
	}

	// A data write is rejected (no PUBACK) and never persisted.
	before := w.st.NextOffset("metrics")
	tk := c.Publish("colca/v1/_Metric/m1/m1/temp", 1, false, `{"v":666}`)
	tk.WaitTimeout(500 * time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	if got := w.st.NextOffset("metrics"); got != before {
		t.Fatalf("human data write persisted: %d → %d", before, got)
	}
}

// The human-session gauge tracks connects and disconnects.
func TestHumanSessionGauge(t *testing.T) {
	w := newHumanWorld(t)
	line := `colca_human_sessions`
	if v := scrapeMetric(t, w.m, line); v != 0 {
		t.Fatalf("%s = %v, want 0", line, v)
	}
	c, err := humanConnect(t, w.srv.HumanTCPAddr(), "ssl", "anna",
		w.iss.Mint("anna", nil, time.Now().Add(time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	if v := scrapeMetric(t, w.m, line); v != 1 {
		t.Fatalf("%s = %v after connect, want 1", line, v)
	}
	c.Disconnect(100)
	deadline := time.Now().Add(5 * time.Second)
	for scrapeMetric(t, w.m, line) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%s stuck at %v after disconnect", line, scrapeMetric(t, w.m, line))
		}
		time.Sleep(20 * time.Millisecond)
	}
}
