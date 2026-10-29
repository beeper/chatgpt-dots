package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func (c *Client) PreHandleMatrixReaction(_ context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error) {
	if msg.TargetMessage == nil || msg.TargetMessage.ID == "" || msg.Portal == nil || msg.TargetMessage.Room != msg.Portal.PortalKey || msg.TargetMessage.Room.Receiver != c.login.ID {
		return bridgev2.MatrixReactionPreResponse{}, errors.New("reaction target belongs to another Dot chat")
	}
	if isTaskItem(msg.TargetMessage.ID) {
		return bridgev2.MatrixReactionPreResponse{}, errors.New("ChatGPT doesn't support reactions on task replies")
	}
	emoji := msg.Content.RelatesTo.Key
	if emoji == "" || strings.HasPrefix(emoji, "mxc://") {
		return bridgev2.MatrixReactionPreResponse{}, errors.New("ChatGPT Dots requires an emoji reaction")
	}
	return bridgev2.MatrixReactionPreResponse{SenderID: c.GetUserID(), Emoji: emoji, MaxReactions: 1}, nil
}

func (c *Client) reactionClient(ctx context.Context, portal *bridgev2.Portal, room networkid.PortalKey) (context.Context, *chatgpt.Client, *RoomState, func(), error) {
	c.mu.RLock()
	api, generation := c.api, c.runContext
	state := c.meta.Rooms[string(room.ID)]
	c.mu.RUnlock()
	if portal == nil || room != portal.PortalKey || room.Receiver != c.login.ID || state == nil {
		return nil, nil, nil, nil, relinkStatus(errors.New("refusing reaction in an unverified Dot room"), true)
	}
	if api == nil || generation == nil || generation.Err() != nil || !c.connected.Load() {
		return nil, nil, nil, nil, wrapSendError(errors.New("not connected to ChatGPT"), true)
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(generation, cancel)
	finish := func() { stop(); cancel() }
	verified, err := api.Verify(ctx, state.Profile)
	if err == nil {
		var current *RoomState
		current, err = roomState(api, state.Profile, verified)
		if err == nil && (current.DotMember != state.DotMember || current.HumanMember != state.HumanMember) {
			err = errors.New("Dot room members changed; refusing reaction")
		}
	}
	if err != nil {
		finish()
		return nil, nil, nil, nil, wrapSendError(err, true)
	}
	return ctx, api, state, finish, nil
}

func (c *Client) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	pre, err := c.PreHandleMatrixReaction(ctx, msg)
	if err != nil {
		return nil, err
	}
	logCtx := ctx
	ctx, api, state, finish, err := c.reactionClient(ctx, msg.Portal, msg.TargetMessage.Room)
	if err != nil {
		c.logReactionFailure(logCtx, err, "verify_room", pre.Emoji, msg.TargetMessage)
		return nil, err
	}
	defer finish()
	if err = api.SetReaction(ctx, state.Profile.Room, string(msg.TargetMessage.ID), &pre.Emoji); err != nil {
		c.logReactionFailure(ctx, err, "add_reaction", pre.Emoji, msg.TargetMessage)
		return nil, wrapSendError(err, false)
	}
	c.requestSync(true)
	return &database.Reaction{Emoji: pre.Emoji}, nil
}

func (c *Client) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	target := msg.TargetReaction
	if target == nil || target.MessageID == "" || target.SenderID != c.GetUserID() {
		return errors.New("refusing to remove another Dot member's reaction")
	}
	logCtx := ctx
	ctx, api, state, finish, err := c.reactionClient(ctx, msg.Portal, target.Room)
	if err != nil {
		c.logReactionFailure(logCtx, err, "verify_room", target.Emoji, nil)
		return err
	}
	defer finish()
	current, err := api.Message(ctx, state.Profile.Room, string(target.MessageID))
	if err != nil {
		c.logReactionFailure(ctx, err, "read_target", target.Emoji, nil)
		return wrapSendError(err, true)
	}
	if current.Reactions[state.HumanMember] != target.Emoji {
		c.requestSync(true)
		return nil
	}
	if err = api.SetReaction(ctx, state.Profile.Room, string(target.MessageID), nil); err != nil {
		c.logReactionFailure(ctx, err, "remove_reaction", target.Emoji, nil)
		return wrapSendError(err, false)
	}
	c.requestSync(true)
	return nil
}

func (c *Client) logReactionFailure(ctx context.Context, err error, stage, emoji string, target *database.Message) {
	codepoints := make([]string, 0, len(emoji))
	for _, r := range emoji {
		codepoints = append(codepoints, fmt.Sprintf("U+%04X", r))
	}
	evt := zerolog.Ctx(ctx).Warn().Err(err).
		Str("stage", stage).
		Str("emoji_codepoints", strings.Join(codepoints, " "))
	var httpErr *chatgpt.HTTPError
	if errors.As(err, &httpErr) {
		evt = evt.Int("status", httpErr.Status).Str("path", httpErr.Path)
	}
	if target != nil {
		sender := "dot"
		if target.SenderID == c.GetUserID() {
			sender = "self"
		}
		evt = evt.Str("target_sender", sender).
			Bool("target_has_part_id", target.PartID != "").
			Bool("target_in_thread", target.ThreadRoot != "").
			Int("target_edit_count", target.EditCount).
			Dur("target_age", time.Since(target.Timestamp))
	}
	evt.Msg("ChatGPT Dot reaction failed")
}

var _ bridgev2.ReactionHandlingNetworkAPI = (*Client)(nil)
