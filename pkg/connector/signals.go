package connector

import (
	"context"
	"errors"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
)

type receiptState struct {
	Native     chatgpt.ReadReceipt
	Target     networkid.MessageID
	TargetTime time.Time
	SentTarget networkid.MessageID
	SentTime   time.Time
}

type nativeReceipt struct {
	simplevent.Receipt
}

func (r *nativeReceipt) GetTimestamp() time.Time { return r.Timestamp }

type nativeTyping struct {
	simplevent.Typing
	expires time.Time
}

func (t *nativeTyping) GetTimeout() time.Duration {
	return max(0, time.Until(t.expires))
}

func (c *Client) requestSync(full bool) {
	if full {
		c.rescan.Store(true)
	}
	select {
	case c.signalWake <- struct{}{}:
	default:
	}
}

func (c *Client) streamSignals(ctx context.Context, api *chatgpt.Client) {
	active := make(map[string]*RoomState)
	clear := func() {
		clearCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		for _, state := range active {
			c.sendTyping(clearCtx, state, 0)
		}
		clear(active)
	}
	defer clear()
	delay := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := api.StreamSignals(ctx, func(signal chatgpt.Signal) error {
			if signal.Type == "resync" {
				clear()
				c.requestSync(true)
				return nil
			}
			state := c.meta.Rooms[signal.Payload.Room]
			if state == nil {
				return nil
			}
			switch signal.Type {
			case "calpico-is-responding-heartbeat":
				if signal.Payload.Source.Role != "user" || signal.Payload.Source.Member != state.DotMember {
					return nil
				}
				timeout := 10 * time.Second
				if signal.Payload.Typing != nil && !*signal.Payload.Typing {
					timeout = 0
				} else if signal.Payload.Timestamp > 0 {
					expires := time.Unix(0, int64(signal.Payload.Timestamp*1e9)).Add(timeout)
					timeout = min(timeout, max(0, time.Until(expires)))
				}
				if timeout > 0 {
					active[signal.Payload.Room] = state
				} else {
					delete(active, signal.Payload.Room)
				}
				return c.sendTyping(ctx, state, timeout)
			case "calpico-message-add":
				if signal.Payload.Message.Member == state.DotMember {
					delete(active, signal.Payload.Room)
					if err := c.sendTyping(ctx, state, 0); err != nil {
						return err
					}
				}
				c.requestSync(false)
			case "calpico-message-update", "calpico-room-leave":
				c.requestSync(true)
			case "calpico-room-read-receipt", "calpico-room-metadata-update":
				c.requestSync(false)
			}
			return nil
		})
		clear()
		if ctx.Err() != nil {
			return
		}
		c.login.Log.Warn().Err(err).Msg("ChatGPT live signals disconnected; message and receipt polling remains active")
		if time.Since(started) >= time.Minute {
			delay = time.Second
		}
		if !wait(ctx, delay) {
			return
		}
		delay = min(delay*2, time.Minute)
	}
}

func (c *Client) sendTyping(ctx context.Context, state *RoomState, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	evt := &nativeTyping{Typing: simplevent.Typing{EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventTyping, PortalKey: c.key(state), Sender: bridgev2.EventSender{Sender: c.dotID(state)}}, Type: bridgev2.TypingTypeText}, expires: time.Now().Add(timeout)}
	evt.MutateContextFunc = func(context.Context) context.Context { return ctx }
	result := c.login.QueueRemoteEvent(evt)
	if !result.Success || result.Ignored {
		return errors.New("could not bridge Dot typing signal")
	}
	return nil
}

func (c *Client) readReceipt(ctx context.Context, api *chatgpt.Client, state *RoomState) (*receiptState, bool, error) {
	room, err := api.RoomSignals(ctx, state.Profile)
	if err != nil {
		return nil, false, err
	}
	dot, human := false, false
	for _, member := range room.Members {
		dot = dot || (member.ID == state.DotMember && member.Aeon == state.Profile.ID)
		human = human || (member.ID == state.HumanMember && member.Aeon == "" && member.ID == api.Identity.User)
	}
	if len(room.Members) != 2 || !dot || !human {
		return nil, false, errors.New("Dot room signal members changed")
	}
	receipt := c.receipts[state.Profile.Room]
	if receipt == nil {
		receipt = &receiptState{}
		c.receipts[state.Profile.Room] = receipt
	}
	changed := false
	for _, native := range room.ReadReceipts {
		if native.Member != state.DotMember || native.ReadAt.IsZero() {
			continue
		}
		if native.ReadAt.After(receipt.Native.ReadAt) || (native.ReadAt.Equal(receipt.Native.ReadAt) && receipt.Native.Updated.IsZero() && !native.Updated.IsZero()) {
			receipt.Native = native
			changed = true
		}
	}
	return receipt, changed, nil
}

func (c *Client) sendReceipt(ctx context.Context, state *RoomState, receipt *receiptState) error {
	if receipt.Target == "" || (receipt.Target == receipt.SentTarget && receipt.Native.Updated.Equal(receipt.SentTime)) {
		return nil
	}
	part, err := c.connector.bridge.DB.Message.GetLastPartByID(ctx, c.login.ID, receipt.Target)
	if err != nil {
		return err
	}
	if part == nil || part.HasFakeMXID() || part.Room != c.key(state) || part.SenderID != c.GetUserID() {
		return errors.New("Dot read receipt target is not a mapped outgoing message")
	}
	evt := &nativeReceipt{simplevent.Receipt{EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventReadReceipt, PortalKey: c.key(state), Sender: bridgev2.EventSender{Sender: c.dotID(state)}, Timestamp: receipt.Native.Updated}, LastTarget: receipt.Target}}
	evt.MutateContextFunc = func(context.Context) context.Context { return ctx }
	result := c.login.QueueRemoteEvent(evt)
	if !result.Success || result.Ignored {
		return errors.New("could not bridge Dot read receipt")
	}
	receipt.SentTarget, receipt.SentTime = receipt.Target, receipt.Native.Updated
	return nil
}

type reactionSnapshot struct {
	simplevent.ReactionSync
	part networkid.PartID
}

func (r *reactionSnapshot) GetTargetMessagePart() networkid.PartID { return r.part }

func (c *Client) syncReactions(ctx context.Context, state *RoomState, m chatgpt.Message) error {
	parts, err := c.connector.bridge.DB.Message.GetAllPartsByID(ctx, c.login.ID, networkid.MessageID(m.ID))
	if err != nil {
		return err
	}
	if len(parts) == 0 || m.Deleted != nil {
		return nil
	}
	partID := textPartID(m, parts)
	if attachments := mappedAttachments(m, parts); len(attachments) > 0 {
		partID = attachments[0].PartID
	}
	emoji := m.Reactions[state.DotMember]
	sender := c.dotID(state)
	readSaved := func() ([]*database.Reaction, error) {
		return c.connector.bridge.DB.Reaction.GetAllToMessageBySender(ctx, c.login.ID, networkid.MessageID(m.ID), sender)
	}
	saved, err := readSaved()
	if err != nil {
		return err
	}
	if len(saved) == 1 {
		for _, part := range parts {
			if part.PartID == saved[0].MessagePartID {
				partID = part.PartID
				break
			}
		}
	}
	matches := func(saved []*database.Reaction) bool {
		if emoji == "" {
			return len(saved) == 0
		}
		return len(saved) == 1 && saved[0].EmojiID == "" && saved[0].Emoji == emoji && saved[0].MessagePartID == partID && saved[0].Room == c.key(state)
	}
	if matches(saved) {
		return nil
	}
	user := &bridgev2.ReactionSyncUser{HasAllReactions: true}
	if emoji != "" {
		user.Reactions = []*bridgev2.BackfillReaction{{Sender: bridgev2.EventSender{Sender: sender}, Emoji: emoji, TargetPart: &partID}}
	}
	evt := &reactionSnapshot{ReactionSync: simplevent.ReactionSync{EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventReactionSync, PortalKey: c.key(state), Sender: bridgev2.EventSender{Sender: sender}}, TargetMessage: networkid.MessageID(m.ID), Reactions: &bridgev2.ReactionSyncData{Users: map[networkid.UserID]*bridgev2.ReactionSyncUser{sender: user}}}, part: partID}
	evt.MutateContextFunc = func(context.Context) context.Context { return ctx }
	result := c.login.QueueRemoteEvent(evt)
	if !result.Success || result.Ignored {
		return errors.New("could not bridge Dot reaction snapshot")
	}
	saved, err = readSaved()
	if err != nil {
		return err
	}
	if !matches(saved) {
		return errors.New("Dot reaction snapshot not durably saved; checkpoint retained")
	}
	return nil
}
