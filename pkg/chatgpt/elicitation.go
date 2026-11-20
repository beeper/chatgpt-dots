package chatgpt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/url"
	"slices"
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

type formSchema struct {
	Type       string                     `json:"type"`
	Properties map[string]json.RawMessage `json:"properties"`
	Required   []string                   `json:"required"`
}

func (e *Elicitation) form(allowedMeta ...string) (map[string]json.RawMessage, *formSchema, bool) {
	if !e.Valid() || (e.Params.Mode != "" && e.Params.Mode != "form") || strings.TrimSpace(e.Params.Message) == "" || e.Params.Server == "browser" || e.Params.Server == "browser-use" {
		return nil, nil, false
	}
	var meta map[string]json.RawMessage
	if len(e.Params.Meta) != 0 && json.Unmarshal(e.Params.Meta, &meta) != nil {
		return nil, nil, false
	}
	for key := range meta {
		if !slices.Contains(allowedMeta, key) {
			return nil, nil, false
		}
	}
	var schema formSchema
	if json.Unmarshal(e.Params.Schema, &schema) != nil || schema.Type != "object" || schema.Properties == nil {
		return nil, nil, false
	}
	return meta, &schema, true
}

func (e *Elicitation) AcceptContent() (map[string]any, bool) {
	if _, ok := e.ToolApproval(); ok {
		return map[string]any{}, true
	}
	meta, schema, ok := e.form("reason", "openai/confirmation")
	if !ok {
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

type Choice struct {
	Label string
	Value any
}

func (e *Elicitation) Choices() (string, []Choice, bool) {
	_, schema, ok := e.form("reason")
	if !ok || len(schema.Properties) != 1 || e.Binary() {
		return "", nil, false
	}
	var name string
	var raw json.RawMessage
	for name, raw = range schema.Properties {
	}
	for _, required := range schema.Required {
		if required != name {
			return "", nil, false
		}
	}
	var keys map[string]json.RawMessage
	if json.Unmarshal(raw, &keys) != nil {
		return "", nil, false
	}
	for key := range keys {
		switch key {
		case "type", "title", "description", "default", "enum", "enumNames", "oneOf", "anyOf":
		default:
			return "", nil, false
		}
	}
	var field struct {
		Type  string            `json:"type"`
		Enum  []string          `json:"enum"`
		Names []string          `json:"enumNames"`
		OneOf []json.RawMessage `json:"oneOf"`
		AnyOf []json.RawMessage `json:"anyOf"`
	}
	if json.Unmarshal(raw, &field) != nil {
		return "", nil, false
	}
	var choices []Choice
	switch {
	case field.Type == "boolean" && keys["enum"] == nil && keys["oneOf"] == nil && keys["anyOf"] == nil:
		choices = []Choice{{"Yes", true}, {"No", false}}
	case field.Type != "string":
		return "", nil, false
	case keys["enum"] != nil && keys["oneOf"] == nil && keys["anyOf"] == nil:
		if keys["enumNames"] != nil && len(field.Names) != len(field.Enum) {
			return "", nil, false
		}
		for i, value := range field.Enum {
			label := value
			if len(field.Names) > 0 {
				label = field.Names[i]
			}
			choices = append(choices, Choice{label, value})
		}
	case (keys["oneOf"] == nil) != (keys["anyOf"] == nil) && keys["enum"] == nil && keys["enumNames"] == nil:
		for _, raw := range append(field.OneOf, field.AnyOf...) {
			var option map[string]json.RawMessage
			var value, title string
			if json.Unmarshal(raw, &option) != nil || !strings.HasPrefix(string(option["const"]), `"`) || json.Unmarshal(option["const"], &value) != nil {
				return "", nil, false
			}
			for key := range option {
				if key != "const" && key != "title" {
					return "", nil, false
				}
			}
			if option["title"] != nil && json.Unmarshal(option["title"], &title) != nil {
				return "", nil, false
			}
			if strings.TrimSpace(title) == "" {
				title = value
			}
			choices = append(choices, Choice{title, value})
		}
	default:
		return "", nil, false
	}
	if len(choices) == 0 {
		return "", nil, false
	}
	for _, choice := range choices {
		if strings.TrimSpace(choice.Label) == "" {
			return "", nil, false
		}
	}
	return name, choices, true
}

// ToolApproval reports whether this is a Codex tool-call approval and which remembered approvals it offers.
func (e *Elicitation) ToolApproval() ([]string, bool) {
	if !e.Valid() || (e.Params.Mode != "" && e.Params.Mode != "form") || strings.TrimSpace(e.Params.Message) == "" {
		return nil, false
	}
	var meta map[string]json.RawMessage
	if json.Unmarshal(e.Params.Meta, &meta) != nil || string(meta["codex_approval_kind"]) != `"mcp_tool_call"` {
		return nil, false
	}
	if input, ok := meta["codex_requires_user_input"]; ok && string(input) != "false" {
		return nil, false
	}
	var schema struct {
		Type       string                     `json:"type"`
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	if json.Unmarshal(e.Params.Schema, &schema) != nil || schema.Type != "object" || schema.Properties == nil || len(schema.Properties) != 0 || len(schema.Required) != 0 {
		return nil, false
	}
	var persist []string
	if raw := meta["persist"]; len(raw) != 0 && string(raw) != "null" {
		var one string
		if json.Unmarshal(raw, &one) == nil {
			persist = []string{one}
		} else if json.Unmarshal(raw, &persist) != nil {
			return nil, false
		}
	}
	for _, option := range persist {
		if option != "session" && option != "always" {
			return nil, false
		}
	}
	return persist, true
}

func (e *Elicitation) Summary() string {
	if e == nil {
		return ""
	}
	text := e.Params.Message
	var meta struct {
		Reason       string          `json:"reason"`
		Confirmation json.RawMessage `json:"openai/confirmation"`
		Params       json.RawMessage `json:"tool_params"`
		Display      []struct {
			Name  string          `json:"display_name"`
			Value json.RawMessage `json:"value"`
		} `json:"tool_params_display"`
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
		if _, ok := e.ToolApproval(); ok {
			if len(meta.Display) == 0 {
				var params map[string]json.RawMessage
				_ = json.Unmarshal(meta.Params, &params)
				for _, name := range slices.Sorted(maps.Keys(params)) {
					meta.Display = append(meta.Display, struct {
						Name  string          `json:"display_name"`
						Value json.RawMessage `json:"value"`
					}{name, params[name]})
				}
			}
			for _, param := range meta.Display {
				value := string(param.Value)
				_ = json.Unmarshal(param.Value, &value)
				if runes := []rune(value); len(runes) > 500 {
					value = string(runes[:500]) + "…"
				}
				if strings.TrimSpace(value) != "" {
					text += "\n\n" + param.Name + ": " + value
				}
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

func (c *Client) RespondElicitation(ctx context.Context, room, message, fingerprint, action, persist string, elicitation *Elicitation) (bool, error) {
	return c.respondElicitation(ctx, room, message, fingerprint, action, persist, -1, elicitation)
}

func (c *Client) RespondElicitationChoice(ctx context.Context, room, message, fingerprint string, choice int, elicitation *Elicitation) (bool, error) {
	if choice < 0 {
		return false, errors.New("invalid ChatGPT choice")
	}
	return c.respondElicitation(ctx, room, message, fingerprint, "accept", "", choice, elicitation)
}

func (c *Client) respondElicitation(ctx context.Context, room, message, fingerprint, action, persist string, choice int, elicitation *Elicitation) (bool, error) {
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
	if choice >= 0 {
		field, choices, ok := elicitation.Choices()
		if !ok || choice >= len(choices) {
			return false, errors.New("ChatGPT did not offer this choice")
		}
		body.Result.Content = map[string]any{field: choices[choice].Value}
	} else if action == "accept" {
		var ok bool
		body.Result.Content, ok = elicitation.AcceptContent()
		if !ok {
			return false, errors.New("confirmation needs the ChatGPT interface")
		}
	}
	if persist != "" {
		offered, _ := elicitation.ToolApproval()
		if action != "accept" || !slices.Contains(offered, persist) {
			return false, errors.New("ChatGPT did not offer this approval option")
		}
		body.Result.Meta = map[string]any{"persist": persist}
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
