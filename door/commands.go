package door

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// ErrDuplicateCommand is returned by PublishCommand when the node already has
// a command with this correlation id from this sender: it is neither stored
// nor run again, and its answer is the first one's.
var ErrDuplicateCommand = errors.New("duplicate command")

// Outcome is the answer to a command the node executed itself (_CmdAdmin,
// _CmdConfigure, ...) or refused as unannounced (404), returned by /publish.
type Outcome struct {
	CorrelationID string          `json:"correlation_id"`
	ResultCode    int             `json:"result_code"`
	Message       string          `json:"message"`
	Result        json.RawMessage `json:"result,omitempty"`
}

// CommandTopic builds colca/v1/<contract>/<node>/<path>/<verb>. An empty path
// addresses the node itself.
func CommandTopic(contract, node, path, verb string) string {
	topic := uns.Prefix() + contract + "/" + node
	if path = strings.Trim(path, "/"); path != "" {
		topic += "/" + path
	}
	return topic + "/" + verb
}

// PublishCommand publishes a command. offset is where it was stored; out is
// set when the node executed the command itself (offset and out both), or
// answered it without storing it: 404 for a verb nobody announced (out and an
// *HTTPError, offset 0). A repeated correlation id is ErrDuplicateCommand.
func (c *Client) PublishCommand(ctx context.Context, topic string, payload any) (offset int64, out *Outcome, err error) {
	body, err := json.Marshal(map[string]any{"topic": topic, "payload": payload})
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/publish", bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("publishing %s: %w", topic, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return 0, nil, fmt.Errorf("publishing %s: %w", topic, err)
	}
	var answer struct {
		Offset    int64    `json:"offset"`
		Duplicate bool     `json:"duplicate"`
		Command   *Outcome `json:"command"`
	}
	decoded := json.Unmarshal(raw, &answer) == nil
	if resp.StatusCode >= 300 {
		var o *Outcome
		if decoded {
			o = answer.Command
		}
		return 0, o, httpResponseError(resp, fmt.Errorf("publishing %s: HTTP %d: %s", topic, resp.StatusCode, truncate(raw, 300)))
	}
	if !decoded {
		return 0, nil, fmt.Errorf("publishing %s: unreadable answer: %s", topic, truncate(raw, 300))
	}
	if answer.Duplicate {
		return 0, nil, fmt.Errorf("publishing %s: %w", topic, ErrDuplicateCommand)
	}
	return answer.Offset, answer.Command, nil
}

// Ack is an _Ack answer as it lies in the commands stream.
type Ack struct {
	CorrelationID string          `json:"correlation_id"`
	ResultCode    int             `json:"result_code"`
	Message       string          `json:"message"`
	Result        json.RawMessage `json:"result,omitempty"`
	Topic         string          `json:"-"`
	ActorID       string          `json:"-"`
}

// ackPollMax bounds the wait between two reads of an idle commands stream.
const ackPollMax = time.Second

// AwaitAck reads the commands stream from offset `from` (usually the
// command's own offset) until the _Ack with correlationID arrives at
// _Ack/<node>/<path>, or ctx ends. 202 answers (queued, forwarded) are
// skipped. path is where the node answers: the command's path for a routed
// command, the verb alone for one the node executes itself; "" takes any
// answer on the node.
//
// It creates no cursor: it reads ahead with from=N under a name it never
// acks, so nothing is left behind that holds back retention.
func (c *Client) AwaitAck(ctx context.Context, node, path, correlationID string, from int64) (Ack, error) {
	if from < 1 {
		return Ack{}, fmt.Errorf("awaiting ack %s: from must be a stream offset (>= 1)", correlationID)
	}
	filter := uns.Prefix() + "_Ack/" + node + "/#"
	if path = strings.Trim(path, "/"); path != "" {
		filter = uns.Prefix() + "_Ack/" + node + "/" + path
	}
	wait := 50 * time.Millisecond
	offset := from
	for {
		page, err := c.fetchAcks(ctx, filter, offset)
		if err != nil {
			return Ack{}, fmt.Errorf("awaiting ack %s: %w", correlationID, err)
		}
		for _, rec := range page.Records {
			var ack Ack
			if json.Unmarshal(rec.Payload, &ack) != nil || ack.CorrelationID != correlationID || ack.ResultCode == 202 {
				continue
			}
			ack.Topic, ack.ActorID = rec.Topic, rec.ActorID
			return ack, nil
		}
		if page.Next > offset {
			// More was read (records or a filtered scan): read on at once.
			offset = page.Next
			wait = 50 * time.Millisecond
			continue
		}
		select {
		case <-ctx.Done():
			return Ack{}, fmt.Errorf("awaiting ack %s: %w", correlationID, ctx.Err())
		case <-time.After(wait):
		}
		wait = min(wait*2, ackPollMax)
	}
}

// fetchAcks reads one page of _Ack records under the ack cursor name. A
// Bearer caller's cursor prefix is its token's subject: a JWT shows it, a PAT
// does not, so the node names it when it refuses the first read (one
// cursor_denied audit record per Client), and the name is kept.
func (c *Client) fetchAcks(ctx context.Context, filter string, offset int64) (Page, error) {
	read := func() (Page, error) {
		return c.FetchWithOptions(ctx, FetchOptions{Stream: commandsStream, Cursor: c.ackCursorName(),
			Max: 1000, Contracts: []string{"_Ack"}, Topics: []string{filter}, From: uint64(offset)}) //nolint:gosec // offset >= 1
	}
	page, err := read()
	var refused *HTTPError
	if err == nil || c.Bearer == "" || c.ackCursor != "" || !errors.As(err, &refused) || refused.StatusCode != http.StatusForbidden {
		return page, err
	}
	const named = "cursors are named "
	msg := refused.Error()
	i := strings.Index(msg, named)
	if i < 0 {
		return page, err
	}
	prefix, _, ok := strings.Cut(msg[i+len(named):], "...")
	if !ok || prefix == "" {
		return page, err
	}
	c.ackCursor = prefix + "await-ack"
	return read()
}

func (c *Client) ackCursorName() string {
	switch {
	case c.ackCursor != "":
		return c.ackCursor
	case c.Bearer == "" && c.Service != "":
		return uns.LocalCursorPrefix + c.Service + "/await-ack"
	case c.Bearer != "":
		if sub := jwtSubject(c.Bearer); sub != "" {
			return sub + "/await-ack"
		}
	}
	return "await-ack" // the admin's own; a PAT learns its prefix
}

// jwtSubject reads a JWT's sub without verifying it: it only names the
// caller's cursor, and the node checks the token itself.
func jwtSubject(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ""
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if json.Unmarshal(raw, &claims) != nil {
		return ""
	}
	return claims.Sub
}
