package door

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestABearerIsSentAsAuthorization(t *testing.T) {
	var auth, token, service string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, token, service = r.Header.Get("Authorization"), r.Header.Get("X-Colca-Token"), r.Header.Get("X-Colca-Service")
		_, _ = w.Write([]byte(`{"stream":"commands","offset":3}`))
	}))
	defer srv.Close()

	if err := (&Client{BaseURL: srv.URL, Bearer: "pk_pat_abc"}).Publish(context.Background(), "colca/v1/_CmdMaintain/n/a/b", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if auth != "Bearer pk_pat_abc" || token != "" || service != "" {
		t.Fatalf("headers: Authorization=%q X-Colca-Token=%q X-Colca-Service=%q", auth, token, service)
	}
}

func TestCommandTopicAddressesAPathOrTheNodeItself(t *testing.T) {
	if got := CommandTopic("_CmdMaintain", "n1", "/_dienste/orchestrator/", "plan"); got != "colca/v1/_CmdMaintain/n1/_dienste/orchestrator/plan" {
		t.Fatalf("path topic = %q", got)
	}
	if got := CommandTopic("_CmdAdmin", "n1", "", "revoke"); got != "colca/v1/_CmdAdmin/n1/revoke" {
		t.Fatalf("node topic = %q", got)
	}
}

func TestPublishCommandReturnsTheNodesOwnOutcome(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"stream":"commands","offset":41,"topic":"t","command":{"correlation_id":"c1","result_code":200,"message":"revoked","result":{"x":1}}}`))
	}))
	defer srv.Close()

	offset, out, err := (&Client{BaseURL: srv.URL, Token: "tok"}).PublishCommand(context.Background(), "colca/v1/_CmdAdmin/n/revoke", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if offset != 41 || out == nil || out.CorrelationID != "c1" || out.ResultCode != 200 || out.Message != "revoked" || string(out.Result) != `{"x":1}` {
		t.Fatalf("offset=%d outcome=%+v", offset, out)
	}
}

func TestPublishCommandOfARoutedCommandHasNoOutcome(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"stream":"commands","offset":7,"topic":"t"}`))
	}))
	defer srv.Close()

	offset, out, err := (&Client{BaseURL: srv.URL}).PublishCommand(context.Background(), "t", map[string]any{})
	if err != nil || offset != 7 || out != nil {
		t.Fatalf("offset=%d outcome=%+v err=%v", offset, out, err)
	}
}

func TestPublishCommandUnannouncedIsAnErrorWithItsOutcome(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"no service executes","command":{"correlation_id":"c2","result_code":404,"message":"no service executes"}}`))
	}))
	defer srv.Close()

	offset, out, err := (&Client{BaseURL: srv.URL}).PublishCommand(context.Background(), "t", map[string]any{})
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != 404 {
		t.Fatalf("err = %v, want HTTP 404", err)
	}
	if offset != 0 || out == nil || out.ResultCode != 404 || out.CorrelationID != "c2" {
		t.Fatalf("offset=%d outcome=%+v", offset, out)
	}
}

func TestPublishCommandDuplicateIsErrDuplicateCommand(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"duplicate":true}`))
	}))
	defer srv.Close()

	if _, _, err := (&Client{BaseURL: srv.URL}).PublishCommand(context.Background(), "t", map[string]any{}); !errors.Is(err, ErrDuplicateCommand) {
		t.Fatalf("err = %v, want ErrDuplicateCommand", err)
	}
}

// ackNode serves the commands stream: a 202 at 11, the answer at 12, and the
// answer becomes visible only on the third read, so AwaitAck has to wait.
type ackNode struct {
	mu      sync.Mutex
	reads   int
	queries []string
}

func (n *ackNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reads++
	n.queries = append(n.queries, r.URL.RawQuery)
	from := r.URL.Query().Get("from")
	switch {
	case from == "10":
		_, _ = w.Write([]byte(`{"records":[{"offset":11,"topic":"colca/v1/_Ack/n1/orch/plan","payload":{"correlation_id":"c9","result_code":202,"message":"queued"}}],"next":12}`))
	case from == "12" && n.reads >= 3:
		_, _ = w.Write([]byte(`{"records":[{"offset":12,"topic":"colca/v1/_Ack/n1/orch/plan","actor_id":"svc","payload":{"correlation_id":"other","result_code":200}},` +
			`{"offset":13,"topic":"colca/v1/_Ack/n1/orch/plan","actor_id":"svc","payload":{"correlation_id":"c9","result_code":200,"message":"planned","result":{"ok":true}}}],"next":14}`))
	default:
		_, _ = w.Write([]byte(`{"records":[],"next":` + from + `}`))
	}
}

func TestAwaitAckSkipsA202AndWaitsForTheAnswer(t *testing.T) {
	node := &ackNode{}
	srv := httptest.NewServer(node)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ack, err := (&Client{BaseURL: srv.URL, Service: "orchestrator"}).AwaitAck(ctx, "n1", "orch/plan", "c9", 10)
	if err != nil {
		t.Fatal(err)
	}
	if ack.ResultCode != 200 || ack.Message != "planned" || string(ack.Result) != `{"ok":true}` ||
		ack.Topic != "colca/v1/_Ack/n1/orch/plan" || ack.ActorID != "svc" {
		t.Fatalf("ack = %+v", ack)
	}
	q := node.queries[0]
	for _, want := range []string{"stream=commands", "contract=_Ack", "from=10", "cursor=c%2Forchestrator%2Fawait-ack",
		"topic=colca%2Fv1%2F_Ack%2Fn1%2Forch%2Fplan"} {
		if !strings.Contains(q, want) {
			t.Fatalf("query %q lacks %q", q, want)
		}
	}
}

func TestAwaitAckEndsWithItsContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"records":[],"next":5}`))
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := (&Client{BaseURL: srv.URL, Token: "tok"}).AwaitAck(ctx, "n1", "", "c1", 5)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the deadline", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("AwaitAck outlived its context by %s", time.Since(start))
	}
}

func TestAwaitAckNamesAJWTsCursorAfterItsSubject(t *testing.T) {
	var cursor string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor = r.URL.Query().Get("cursor")
		_, _ = w.Write([]byte(`{"records":[{"offset":5,"topic":"colca/v1/_Ack/n1/revoke","payload":{"correlation_id":"c1","result_code":200}}],"next":6}`))
	}))
	defer srv.Close()
	claims := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"user-42"}`))

	if _, err := (&Client{BaseURL: srv.URL, Bearer: "h." + claims + ".s"}).AwaitAck(context.Background(), "n1", "revoke", "c1", 5); err != nil {
		t.Fatal(err)
	}
	if cursor != "user-42/await-ack" {
		t.Fatalf("cursor = %q", cursor)
	}
}

func TestAwaitAckLearnsAPATsCursorPrefixFromTheNode(t *testing.T) {
	var cursors []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := r.URL.Query().Get("cursor")
		cursors = append(cursors, cursor)
		if !strings.HasPrefix(cursor, "owner-sub/") {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"cursor not owned: this identity's cursors are named owner-sub/..."}`))
			return
		}
		_, _ = w.Write([]byte(`{"records":[{"offset":5,"topic":"colca/v1/_Ack/n1/revoke","payload":{"correlation_id":"c1","result_code":200}}],"next":6}`))
	}))
	defer srv.Close()

	c := &Client{BaseURL: srv.URL, Bearer: "pk_pat_x"}
	for range 2 {
		if _, err := c.AwaitAck(context.Background(), "n1", "revoke", "c1", 5); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Join(cursors, ",") != "await-ack,owner-sub/await-ack,owner-sub/await-ack" {
		t.Fatalf("cursors = %v", cursors)
	}
}
