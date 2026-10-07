package door

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A node that answers 409 store_changed is reported as ErrStoreChanged, and the
// store id the caller read from is sent with the ack.
func TestAckStoreReportsAChangedStore(t *testing.T) {
	var sent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sent = string(body)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"store changed","store":"new","store_changed":true}`))
	}))
	defer srv.Close()
	c := &Client{BaseURL: srv.URL}
	_, err := c.AckStore(context.Background(), "metrics", "c/x", 5, "old")
	if !errors.Is(err, ErrStoreChanged) {
		t.Fatalf("err = %v, want ErrStoreChanged", err)
	}
	if !strings.Contains(sent, `"store":"old"`) {
		t.Fatalf("sent %s, want the store id", sent)
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusConflict {
		t.Fatalf("err = %v, want an HTTPError 409", err)
	}
}
