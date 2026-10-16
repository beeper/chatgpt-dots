package chatgpt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

// Elicitation is a native confirmation, separate from the message's text and attachments.
type Elicitation struct {
	RequestID string `json:"request_id"`
	ThreadID  string `json:"thread_id"`
	TurnID    string `json:"turn_id"`
	Status    string `json:"status,omitempty"`
	Params    struct {
		ThreadID string          `json:"threadId"`
		TurnID   string          `json:"turnId"`
		Server   string          `json:"serverName"`
		Mode     string          `json:"mode"`
		Message  string          `json:"message"`
		Schema   json.RawMessage `json:"requestedSchema"`
		Meta     json.RawMessage `json:"_meta"`
	} `json:"params"`
}

func (e *Elicitation) Valid() bool {
	return e != nil && strings.TrimSpace(e.RequestID) != "" && strings.TrimSpace(e.ThreadID) != "" &&
		e.Params.ThreadID == e.ThreadID && (e.Params.TurnID == "" || e.Params.TurnID == e.TurnID)
}

// Binary confirmations need no form data. Other requests must stay in the native UI.
func (e *Elicitation) Binary() bool {
	_, ok := e.AcceptContent()
	return ok
}

func (e *Elicitation) AcceptContent() (map[string]any, bool) {
	if !e.Valid() || (e.Params.Mode != "" && e.Params.Mode != "form") || strings.TrimSpace(e.Params.Message) == "" {
		return nil, false
	}
	if e.Params.Server == "browser" || e.Params.Server == "browser-use" {
		return nil, false
	}
	var meta map[string]json.RawMessage
	if len(e.Params.Meta) != 0 && json.Unmarshal(e.Params.Meta, &meta) != nil {
		return nil, false
	}
	for key := range meta {
		if key != "reason" && key != "openai/confirmation" {
			return nil, false
		}
	}
	var schema struct {
		Type       string                     `json:"type"`
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	if json.Unmarshal(e.Params.Schema, &schema) != nil || schema.Type != "object" || schema.Properties == nil {
		return nil, false
	}
	if len(schema.Properties) == 0 && len(schema.Required) == 0 {
		return map[string]any{}, true
	}
	var confirmation struct {
		Version int    `json:"version"`
		Field   string `json:"field"`
	}
	if json.Unmarshal(meta["openai/confirmation"], &confirmation) != nil || confirmation.Version != 1 || confirmation.Field == "" || len(schema.Properties) != 1 || len(schema.Required) != 1 || schema.Required[0] != confirmation.Field {
		return nil, false
	}
	var field map[string]json.RawMessage
	if json.Unmarshal(schema.Properties[confirmation.Field], &field) != nil || string(field["type"]) != `"boolean"` {
		return nil, false
	}
	for key := range field {
		if key != "type" && key != "title" && key != "description" && key != "default" {
			return nil, false
		}
	}
	return map[string]any{confirmation.Field: true}, true
}

func (e *Elicitation) Summary() string {
	if e == nil {
		return ""
	}
	text := e.Params.Message
	var meta struct {
		Reason       string          `json:"reason"`
		Confirmation json.RawMessage `json:"openai/confirmation"`
	}
	var confirmation struct {
		Presentation struct {
			Title  string `json:"title"`
			Fields []struct {
				Label string `json:"label"`
				Value string `json:"value"`
			} `json:"fields"`
		} `json:"presentation"`
	}
	if json.Unmarshal(e.Params.Meta, &meta) == nil {
		_ = json.Unmarshal(meta.Confirmation, &confirmation)
		if strings.TrimSpace(meta.Reason) != "" && !strings.Contains(text, meta.Reason) {
			text += "\n\n" + meta.Reason
		}
		for _, field := range confirmation.Presentation.Fields {
			if strings.TrimSpace(field.Value) != "" && !strings.Contains(text, field.Value) {
				text += "\n\n" + field.Label + " " + field.Value
			}
		}
	}
	return text
}

// Definition excludes runtime status so resolving a request does not change its identity.
func (e *Elicitation) Definition() string {
	if !e.Valid() {
		return ""
	}
	copy := *e
	copy.Status = ""
	// Normalize nested JSON objects before binding what the user was shown.
	raw, _ := json.Marshal(copy)
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return ""
	}
	raw, _ = json.Marshal(value)
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

type ElicitationState struct {
	Elicitation *Elicitation `json:"elicitation"`
	Fingerprint string       `json:"request_fingerprint"`
	CanRespond  bool         `json:"can_respond"`
	Runtime     string       `json:"runtime_status"`
}

func (c *Client) elicitationPath(room, message string) (string, error) {
	c.mu.RLock()
	verified := c.rooms[room]
	c.mu.RUnlock()
	if !verified || message == "" {
		return "", errors.New("refusing confirmation outside a verified Dot room")
	}
	return "/messaging/rooms/" + url.PathEscape(room) + "/messages/" + url.PathEscape(message) + "/elicitation", nil
}

func (c *Client) ReadElicitation(ctx context.Context, room, message string) (*ElicitationState, error) {
	path, err := c.elicitationPath(room, message)
	if err != nil {
		return nil, err
	}
	var state ElicitationState
	if err = c.do(ctx, "GET", path+"?inspect=true", nil, &state); err != nil {
		return nil, err
	}
	if !state.Elicitation.Valid() || state.Fingerprint == "" || state.Runtime == "" {
		return nil, errors.New("ChatGPT returned an incomplete confirmation state")
	}
	return &state, nil
}

func (c *Client) RespondElicitation(ctx context.Context, room, message, fingerprint, action string, elicitation *Elicitation) (bool, error) {
	path, err := c.elicitationPath(room, message)
	if err != nil {
		return false, err
	}
	if fingerprint == "" || (action != "accept" && action != "decline") {
		return false, errors.New("invalid ChatGPT confirmation response")
	}
	body := struct {
		Fingerprint string `json:"request_fingerprint"`
		Result      struct {
			Action  string         `json:"action"`
			Content map[string]any `json:"content"`
			Meta    map[string]any `json:"_meta"`
		} `json:"result"`
	}{Fingerprint: fingerprint}
	body.Result.Action = action
	if action == "accept" {
		var ok bool
		body.Result.Content, ok = elicitation.AcceptContent()
		if !ok {
			return false, errors.New("confirmation needs the ChatGPT interface")
		}
	}
	var response struct {
		Status string `json:"status"`
	}
	// Do not retry a confirmation POST, even after a 401: the user action may
	// already have reached the runtime. The caller persists its reservation first.
	c.authMu.Lock()
	defer c.authMu.Unlock()
	err = c.request(ctx, "POST", "https://chatgpt.com/backend-api"+path+"/response", body, &response)
	return err == nil && response.Status == "applied", err
}
