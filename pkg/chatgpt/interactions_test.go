package chatgpt

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestNativeMessageReplyFields(t *testing.T) {
	var message Message
	if err := json.Unmarshal([]byte(`{"id":"reply","reply_to":{"message_id":"parent"},"reply_root_message_id":"root"}`), &message); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["reply_to"]) != `{"message_id":"parent"}` || string(fields["reply_root_message_id"]) != `"root"` {
		t.Fatalf("native reply relationship was discarded: %s", encoded)
	}
}

func interactionClient(handler func(*http.Request) (*http.Response, error)) *Client {
	return &Client{rooms: map[string]bool{"room": true}, Identity: Identity{Expires: time.Now().Add(time.Hour)}, http: &http.Client{Transport: confirmationTransport(handler)}}
}

func interactionResponse(r *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Request: r, Header: http.Header{}}
}

func TestNativeReplySendWire(t *testing.T) {
	for _, target := range []string{"", "parent"} {
		t.Run(target, func(t *testing.T) {
			calls := 0
			c := interactionClient(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != "POST" || r.URL.Path != "/backend-api/messaging/rooms/room/messages" {
					t.Fatalf("unexpected request: %s %s", r.Method, r.URL)
				}
				var body struct {
					Content struct {
						Text string `json:"text"`
					} `json:"content"`
					Request     string          `json:"request_id"`
					Idempotency string          `json:"idempotency_token"`
					Reply       json.RawMessage `json:"reply_to"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body.Content.Text != "answer" || body.Request != "request" || body.Idempotency != "request" {
					t.Fatalf("invalid send payload: %+v", body)
				}
				if target == "" && body.Reply != nil || target != "" && string(body.Reply) != `{"message_id":"parent"}` {
					t.Fatalf("invalid reply target %s", body.Reply)
				}
				return interactionResponse(r, 200, `{"id":"sent","request_id":"request"}`), nil
			})
			message, err := c.Send(context.Background(), "room", "answer", "request", nil, target)
			if err != nil || message.ID != "sent" || calls != 1 {
				t.Fatalf("message=%+v error=%v calls=%d", message, err, calls)
			}
		})
	}
}

func TestNativeReactionWire(t *testing.T) {
	for _, emoji := range []string{"👍", "🔥", ""} {
		t.Run(emoji, func(t *testing.T) {
			calls := 0
			c := interactionClient(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.Path != "/backend-api/messaging/rooms/room/messages/message/reactions" {
					t.Fatalf("unexpected path: %s", r.URL)
				}
				if emoji == "" {
					if r.Method != "DELETE" || r.Body != nil {
						t.Fatalf("delete was %s with body %v", r.Method, r.Body)
					}
				} else {
					var body map[string]string
					if r.Method != "POST" {
						t.Fatalf("add was %s", r.Method)
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Fatal(err)
					}
					if len(body) != 1 || body["reaction"] != emoji {
						t.Fatalf("unexpected reaction payload: %v", body)
					}
				}
				return interactionResponse(r, 204, ""), nil
			})
			var value *string
			if emoji != "" {
				value = &emoji
			}
			if err := c.SetReaction(context.Background(), "room", "message", value); err != nil || calls != 1 {
				t.Fatalf("error=%v calls=%d", err, calls)
			}
		})
	}
}

func TestNativeReplyHistoryWire(t *testing.T) {
	for _, before := range []string{"", "older"} {
		c := interactionClient(func(r *http.Request) (*http.Response, error) {
			q := r.URL.Query()
			if r.Method != "GET" || r.URL.Path != "/backend-api/messaging/rooms/room/messages" || q.Get("limit") != "32" || q.Get("reply_root_message_id") != "root" || q.Get("before") != before || q.Has("after") {
				t.Fatalf("unexpected reply history request: %s %s", r.Method, r.URL)
			}
			return interactionResponse(r, 200, `{"items":[{"id":"reply","reply_root_message_id":"root"}],"prev_cursor":"previous"}`), nil
		})
		page, err := c.Replies(context.Background(), "room", "root", before)
		if err != nil || len(page.Items) != 1 || page.Items[0].ReplyRootMessageID != "root" || page.Prev != "previous" {
			t.Fatalf("page=%+v error=%v", page, err)
		}
	}
}

func TestNativeMessageLookupSelectsExactID(t *testing.T) {
	for _, found := range []bool{true, false} {
		c := interactionClient(func(r *http.Request) (*http.Response, error) {
			q := r.URL.Query()
			if r.Method != "GET" || q.Get("around") != "target" || q.Get("limit") != "32" {
				t.Fatalf("unexpected message lookup: %s %s", r.Method, r.URL)
			}
			body := `{"items":[{"id":"neighbor"}]}`
			if found {
				body = `{"items":[{"id":"neighbor"},{"id":"target"}]}`
			}
			return interactionResponse(r, 200, body), nil
		})
		message, err := c.Message(context.Background(), "room", "target")
		if found && (err != nil || message.ID != "target") || !found && (err == nil || message != nil) {
			t.Fatalf("found=%v message=%+v error=%v", found, message, err)
		}
	}
}

func TestInteractionsRequireVerifiedRoomAndTarget(t *testing.T) {
	calls := 0
	c := interactionClient(func(r *http.Request) (*http.Response, error) { calls++; return interactionResponse(r, 200, `{}`), nil })
	for _, room := range []string{"room", "other"} {
		target := ""
		if room == "other" {
			target = "target"
		}
		if _, err := c.Replies(context.Background(), room, target, ""); err == nil {
			t.Fatal("invalid reply history accepted")
		}
		if _, err := c.Message(context.Background(), room, target); err == nil {
			t.Fatal("invalid message lookup accepted")
		}
		if err := c.SetReaction(context.Background(), room, target, nil); err == nil {
			t.Fatal("invalid reaction accepted")
		}
	}
	if _, err := c.Send(context.Background(), "other", "text", "request", nil, "target"); err == nil {
		t.Fatal("unverified send accepted")
	}
	if calls != 0 {
		t.Fatalf("invalid interactions reached provider %d times", calls)
	}
}
