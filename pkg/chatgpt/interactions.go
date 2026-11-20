package chatgpt

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

type ReplyTarget struct {
	MessageID string `json:"message_id"`
}

func (c *Client) interactionRoom(room, messageID string) error {
	c.mu.RLock()
	verified := c.rooms[room]
	c.mu.RUnlock()
	if !verified || messageID == "" {
		return errors.New("refusing unverified Dot room or missing message identity")
	}
	return nil
}

func (c *Client) Replies(ctx context.Context, room, root, before string) (*Page, error) {
	if err := c.interactionRoom(room, root); err != nil {
		return nil, err
	}
	query := url.Values{"limit": {"32"}, "reply_root_message_id": {root}}
	if before != "" {
		query.Set("before", before)
	}
	var page Page
	err := c.do(ctx, http.MethodGet, "/messaging/rooms/"+url.PathEscape(room)+"/messages?"+query.Encode(), nil, &page)
	return &page, err
}

var ErrMessageNotFound = errors.New("ChatGPT did not return the requested message")

func (c *Client) Message(ctx context.Context, room, messageID string) (*Message, error) {
	message, _, err := c.MessageAndPrevious(ctx, room, messageID)
	return message, err
}

func (c *Client) MessageAndPrevious(ctx context.Context, room, messageID string) (*Message, string, error) {
	if err := c.interactionRoom(room, messageID); err != nil {
		return nil, "", err
	}
	query := url.Values{"limit": {"32"}, "around": {messageID}}
	var page Page
	if err := c.do(ctx, http.MethodGet, "/messaging/rooms/"+url.PathEscape(room)+"/messages?"+query.Encode(), nil, &page); err != nil {
		return nil, "", err
	}
	sort.SliceStable(page.Items, func(i, j int) bool { return page.Items[i].Created.Before(page.Items[j].Created) })
	for i, message := range page.Items {
		if message.ID == messageID {
			previous := ""
			if i > 0 {
				previous = page.Items[i-1].ID
			}
			return &message, previous, nil
		}
	}
	return nil, "", ErrMessageNotFound
}

func (c *Client) SetReaction(ctx context.Context, room, messageID string, emoji *string) error {
	if err := c.interactionRoom(room, messageID); err != nil {
		return err
	}
	path := "/messaging/rooms/" + url.PathEscape(room) + "/messages/" + url.PathEscape(messageID) + "/reactions"
	if emoji == nil {
		return c.do(ctx, http.MethodDelete, path, nil, nil)
	}
	return c.do(ctx, http.MethodPost, path, struct {
		Reaction string `json:"reaction"`
	}{Reaction: *emoji}, nil)
}

type OptionPicker struct {
	Options  []string `json:"options"`
	Selected *int     `json:"selected"`
}

func (m *Message) OptionPicker() (*OptionPicker, bool) {
	var picker OptionPicker
	if json.Unmarshal(m.Metadata["option_picker"], &picker) != nil || len(picker.Options) == 0 {
		return nil, false
	}
	for _, option := range picker.Options {
		if strings.TrimSpace(option) == "" {
			return nil, false
		}
	}
	if picker.Selected != nil && (*picker.Selected < -1 || *picker.Selected >= len(picker.Options)) {
		return nil, false
	}
	return &picker, true
}

func (m *Message) OptionPickerSelection() (int, bool) {
	var reply struct {
		Selection *int `json:"selection"`
	}
	if json.Unmarshal(m.Metadata["option_picker"], &reply) != nil || reply.Selection == nil || *reply.Selection < 0 || m.ReplyTo == nil || m.ReplyTo.MessageID == "" {
		return 0, false
	}
	return *reply.Selection, true
}

func (c *Client) AnswerOptionPicker(ctx context.Context, room, messageID, requestID, option string, selection int) (*Message, error) {
	if err := c.interactionRoom(room, messageID); err != nil {
		return nil, err
	}
	if requestID == "" || strings.TrimSpace(option) == "" || selection < 0 {
		return nil, errors.New("invalid ChatGPT option selection")
	}
	body := map[string]any{
		"content":           map[string]any{"text": option},
		"request_id":        requestID,
		"idempotency_token": requestID,
		"reply_to":          ReplyTarget{MessageID: messageID},
		"message_metadata":  map[string]any{"option_picker": map[string]any{"selection": selection}},
	}
	var m Message
	c.authMu.Lock()
	defer c.authMu.Unlock()
	err := c.request(ctx, http.MethodPost, "https://chatgpt.com/backend-api/messaging/rooms/"+url.PathEscape(room)+"/messages", body, &m)
	if err == nil && (m.ID == "" || m.RequestID != requestID) {
		err = errors.New("ChatGPT did not return the selection message")
	}
	return &m, err
}
