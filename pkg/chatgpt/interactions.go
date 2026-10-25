package chatgpt

import (
	"context"
	"errors"
	"net/http"
	"net/url"
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

func (c *Client) Message(ctx context.Context, room, messageID string) (*Message, error) {
	if err := c.interactionRoom(room, messageID); err != nil {
		return nil, err
	}
	query := url.Values{"limit": {"32"}, "around": {messageID}}
	var page Page
	if err := c.do(ctx, http.MethodGet, "/messaging/rooms/"+url.PathEscape(room)+"/messages?"+query.Encode(), nil, &page); err != nil {
		return nil, err
	}
	for _, message := range page.Items {
		if message.ID == messageID {
			return &message, nil
		}
	}
	return nil, errors.New("ChatGPT did not return the requested message")
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
