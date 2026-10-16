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

type confirmationTransport func(*http.Request) (*http.Response, error)

func (f confirmationTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestElicitationWireContract(t *testing.T) {
	for _, action := range []string{"accept", "decline"} {
		t.Run(action, func(t *testing.T) {
			calls := 0
			c := &Client{rooms: map[string]bool{"room": true}, Identity: Identity{Expires: time.Now().Add(time.Hour)}}
			c.http = &http.Client{Transport: confirmationTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != "POST" || r.URL.Path != "/backend-api/messaging/rooms/room/messages/message/elicitation/response" {
					t.Fatalf("unexpected route %s %s", r.Method, r.URL.Path)
				}
				var body map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				var result map[string]json.RawMessage
				_ = json.Unmarshal(body["result"], &result)
				content := "null"
				if action == "accept" {
					content = "{}"
				}
				if string(result["action"]) != `"`+action+`"` || string(result["content"]) != content || string(result["_meta"]) != "null" || string(body["request_fingerprint"]) != `"fresh"` {
					t.Fatal("incorrect native confirmation payload")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"status":"applied"}`)), Request: r}, nil
			})}
			applied, err := c.RespondElicitation(context.Background(), "room", "message", "fresh", action)
			if err != nil || !applied || calls != 1 {
				t.Fatalf("applied=%v err=%v calls=%d", applied, err, calls)
			}
			if _, err = c.RespondElicitation(context.Background(), "other", "message", "fresh", action); err == nil || calls != 1 {
				t.Fatal("unverified room reached provider")
			}
		})
	}
}

func TestElicitationDoesNotRetryOrInferApplied(t *testing.T) {
	for _, response := range []struct {
		status int
		body   string
	}{{401, `{}`}, {200, `{"status":"unconfirmed"}`}, {200, `{"status":"resolved"}`}} {
		calls := 0
		c := &Client{rooms: map[string]bool{"room": true}}
		c.http = &http.Client{Transport: confirmationTransport(func(r *http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: response.status, Body: io.NopCloser(strings.NewReader(response.body)), Request: r, Header: http.Header{}}, nil
		})}
		applied, _ := c.RespondElicitation(context.Background(), "room", "message", "fresh", "accept")
		if applied || calls != 1 {
			t.Fatalf("status=%d body=%s applied=%v calls=%d", response.status, response.body, applied, calls)
		}
	}
}

func TestElicitationDefinitionAndSupportedRequests(t *testing.T) {
	var m Message
	if err := json.Unmarshal([]byte(`{"id":"card","content":{"text":"Save custom rule?","elicitation":{"request_id":"request","thread_id":"thread","turn_id":"turn","status":"pending","params":{"threadId":"thread","turnId":"turn","serverName":"custom_user_rules","mode":"form","message":"Save custom rule?","requestedSchema":{"type":"object","properties":{}}}}}}`), &m); err != nil {
		t.Fatal(err)
	}
	e := m.Content.Elicitation
	if !e.Binary() {
		t.Fatal("plain confirmation not supported")
	}
	definition := e.Definition()
	e.Status = "resolved"
	if e.Definition() != definition {
		t.Fatal("runtime status changed request identity")
	}
	e.Params.Schema = json.RawMessage(`{"properties":{},"type":"object"}`)
	if e.Definition() != definition {
		t.Fatal("JSON key ordering changed request identity")
	}
	e.Params.Message = "A different rule"
	if e.Definition() == definition {
		t.Fatal("changed rule retained request identity")
	}
	e.Params.Schema = json.RawMessage(`{"type":"object","properties":{"code":{"type":"string"}}}`)
	if e.Binary() {
		t.Fatal("form fields offered binary approval")
	}
	e.Params.Schema = json.RawMessage(`{"type":"object","properties":{}}`)
	e.Params.Mode = "openai/userVerification"
	if e.Binary() {
		t.Fatal("verification offered binary approval")
	}
	e.Params.Mode = "form"
	e.Params.Meta = json.RawMessage(`{"openai/location":{}}`)
	if e.Binary() {
		t.Fatal("location offered binary approval")
	}
	e.Params.Meta = nil
	for _, meta := range []string{`{"openai/elicitation":{"requestedSchema":{"type":"object","properties":{"code":{"type":"string"}}}}}`, `{"codex_approval_kind":"browser_auth"}`, `{"persist":"always","origin":"https://example.com"}`, `[]`, `42`, `"unsupported"`} {
		e.Params.Meta = json.RawMessage(meta)
		if e.Binary() {
			t.Fatalf("special metadata offered binary approval: %s", meta)
		}
		var roundtrip Elicitation
		encoded, _ := json.Marshal(e)
		if json.Unmarshal(encoded, &roundtrip) != nil {
			t.Fatal("unsupported metadata broke message decoding")
		}
	}
	e.Params.Meta = json.RawMessage(`{"reason":"The exact action being approved","openai/confirmation":true}`)
	if !e.Binary() || !strings.Contains(e.Summary(), "The exact action being approved") {
		t.Fatal("confirmation reason lost")
	}
	e.Params.Meta = nil
	e.Params.ThreadID = "different"
	if e.Valid() || e.Definition() != "" {
		t.Fatal("conflicting request identity accepted")
	}
}
