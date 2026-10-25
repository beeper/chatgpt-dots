package connector

import (
	"context"
	"errors"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func (c *Client) findReplyRequest(ctx context.Context, api *chatgpt.Client, state *RoomState, requestID, replyTo string) (*chatgpt.Message, error) {
	target, err := api.Message(ctx, state.Profile.Room, replyTo)
	if err != nil {
		return nil, err
	}
	root := target.ReplyRootMessageID
	if root == "" {
		root = target.ID
	}
	before := ""
	seen := make(map[string]bool)
	for !seen[before] {
		seen[before] = true
		page, err := api.Replies(ctx, state.Profile.Room, root, before)
		if err != nil {
			return nil, err
		}
		for _, message := range page.Items {
			if message.RequestID == requestID && message.Sender == state.HumanMember {
				return &message, nil
			}
		}
		if page.Prev == "" {
			return nil, nil
		}
		before = page.Prev
	}
	return nil, errors.New("reply pagination repeated; refusing another POST")
}

func (c *Client) matrixReplyTarget(msg *bridgev2.MatrixMessage) (string, error) {
	for _, target := range []*database.Message{msg.ReplyTo, msg.ThreadRoot} {
		if target != nil && (target.ID == "" || target.Room != msg.Portal.PortalKey || target.Room.Receiver != c.login.ID) {
			return "", errors.New("reply target belongs to another Dot chat")
		}
	}
	if msg.ReplyTo != nil {
		return string(msg.ReplyTo.ID), nil
	} else if msg.ThreadRoot != nil {
		return string(msg.ThreadRoot.ID), nil
	} else if msg.Content.RelatesTo != nil && (msg.Content.RelatesTo.GetReplyTo() != "" || msg.Content.RelatesTo.GetThreadParent() != "") {
		return "", errors.New("reply target has not been bridged to ChatGPT")
	}
	return "", nil
}

func nativeReply(m chatgpt.Message, result *bridgev2.ConvertedMessage) {
	if m.ReplyTo != nil && m.ReplyTo.MessageID != "" {
		result.ReplyTo = &networkid.MessageOptionalPartID{MessageID: networkid.MessageID(m.ReplyTo.MessageID)}
	}
	if m.ReplyRootMessageID != "" {
		root := networkid.MessageID(m.ReplyRootMessageID)
		result.ThreadRoot = &root
	}
}
