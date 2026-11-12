package connector

import (
	"context"
	"errors"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"github.com/coder/websocket"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
)

type receiptState struct {
	Native     chatgpt.ReadReceipt
	Human      chatgpt.ReadReceipt
	SentReadAt time.Time
	SentTime   time.Time
}

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

func (c *Client) requestRefresh(room, messageID string, changed bool) {
	if messageID != "" {
		c.refreshMu.Lock()
		if c.refresh == nil {
			c.refresh = make(map[string]map[string]bool)
		}
		if c.refresh[room] == nil {
			c.refresh[room] = make(map[string]bool)
		}
		c.refresh[room][messageID] = c.refresh[room][messageID] || changed
		c.refreshMu.Unlock()
	}
	c.requestSync(false)
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
				c.recheck.Store(true)
				c.requestSync(false)
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
				c.sendTyping(ctx, state, timeout)
			case "calpico-message-add":
				if signal.Payload.Message.Member == state.DotMember {
					delete(active, signal.Payload.Room)
					c.sendTyping(ctx, state, 0)
				}
				c.requestRefresh(signal.Payload.Room, signal.Payload.Message.ID, false)
			case "calpico-message-update", "calpico-message-task-associated":
				c.requestRefresh(signal.Payload.Room, signal.Payload.MessageID, true)
			case "calpico-room-read-receipt", "calpico-room-read", "calpico-room-metadata-update":
				c.requestSync(false)
			case "calpico-room-leave":
				c.requestSync(true)
			}
			return nil
		})
		c.recheck.Store(true)
		c.requestSync(false)
		clear()
		if ctx.Err() != nil {
			return
		}
		evt := c.login.Log.Warn().Err(err)
		var disconnected chatgpt.StreamDisconnectedError
		if errors.As(err, &disconnected) {
			if disconnected.CloseStatus == websocket.StatusNormalClosure && disconnected.Subscribed && !disconnected.PingFailed {
				evt = c.login.Log.Debug().Err(err)
			}
			evt = evt.Int("close_status", int(disconnected.CloseStatus)).
				Bool("ping_failed", disconnected.PingFailed).
				Bool("subscribed", disconnected.Subscribed).
				Dur("connected_for", disconnected.Connected).
				Str("read_error", disconnected.ReadError)
		}
		evt.Msg("ChatGPT live signals disconnected; message and receipt polling remains active")
		if time.Since(started) >= time.Minute {
			delay = time.Second
		}
		if !wait(ctx, delay) {
			return
		}
		delay = min(delay*2, time.Minute)
	}
}

func (c *Client) sendTyping(ctx context.Context, state *RoomState, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	evt := &nativeTyping{Typing: simplevent.Typing{EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventTyping, PortalKey: c.key(state), Sender: bridgev2.EventSender{Sender: c.dotID(state)}}, Type: bridgev2.TypingTypeText}, expires: time.Now().Add(timeout)}
	evt.MutateContextFunc = func(context.Context) context.Context { return ctx }
	c.login.QueueRemoteEvent(evt)
}

func (c *Client) readReceipt(ctx context.Context, api *chatgpt.Client, state *RoomState) (*receiptState, error) {
	room, err := api.RoomSignals(ctx, state.Profile)
	if err != nil {
		return nil, c.checkDotReplaced(ctx, api, state, err)
	}
	dot, human := false, false
	for _, member := range room.Members {
		dot = dot || (member.ID == state.DotMember && member.Aeon == state.Profile.ID)
		human = human || (member.ID == state.HumanMember && member.Aeon == "" && api.Identity.IsSelf(member.ID))
	}
	if len(room.Members) != 2 || !dot || !human {
		return nil, errors.New("Dot room signal members changed")
	}
	receipt := c.receipts[state.Profile.Room]
	if receipt == nil {
		receipt = &receiptState{}
		c.receipts[state.Profile.Room] = receipt
	}
	for _, native := range room.ReadReceipts {
		if native.ReadAt.IsZero() {
			continue
		}
		current := &receipt.Native
		if native.Member == state.HumanMember {
			current = &receipt.Human
		} else if native.Member != state.DotMember {
			continue
		}
		if native.ReadAt.After(current.ReadAt) || (native.ReadAt.Equal(current.ReadAt) && current.Updated.IsZero() && !native.Updated.IsZero()) {
			*current = native
		}
	}
	return receipt, nil
}

func (c *Client) sendReceipt(ctx context.Context, state *RoomState, receipt *receiptState) error {
	if receipt.Native.ReadAt.IsZero() || (receipt.Native.ReadAt.Equal(receipt.SentReadAt) && receipt.Native.Updated.Equal(receipt.SentTime)) {
		return nil
	}
	target, err := c.connector.bridge.DB.Message.GetLastNonFakePartAtOrBeforeTime(ctx, c.key(state), receipt.Native.ReadAt)
	if err != nil {
		return err
	}
	if target == nil {
		return nil
	}
	evt := &simplevent.Receipt{EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventReadReceipt, PortalKey: c.key(state), Sender: bridgev2.EventSender{Sender: c.dotID(state)}, Timestamp: receipt.Native.Updated}, ReadUpTo: receipt.Native.ReadAt}
	evt.MutateContextFunc = func(context.Context) context.Context { return ctx }
	result := c.login.QueueRemoteEvent(evt)
	if result.Ignored {
		return nil
	}
	if !result.Success {
		return errors.New("could not bridge Dot read receipt")
	}
	receipt.SentReadAt, receipt.SentTime = receipt.Native.ReadAt, receipt.Native.Updated
	return nil
}

func (c *Client) sendHumanReceipt(ctx context.Context, state *RoomState, receipt *receiptState) error {
	c.mu.RLock()
	bridged := state.HumanRead
	c.mu.RUnlock()
	if !receipt.Human.ReadAt.After(bridged) {
		return nil
	}
	if !bridged.IsZero() {
		evt := &simplevent.Receipt{EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventReadReceipt, PortalKey: c.key(state), Sender: bridgev2.EventSender{Sender: c.GetUserID(), SenderLogin: c.login.ID, IsFromMe: true}, Timestamp: receipt.Human.Updated}, ReadUpTo: receipt.Human.ReadAt}
		evt.MutateContextFunc = func(context.Context) context.Context { return ctx }
		if result := c.login.QueueRemoteEvent(evt); !result.Success && !result.Ignored {
			return errors.New("could not bridge own ChatGPT read receipt")
		}
	}
	return c.saveHumanRead(ctx, state, receipt.Human.ReadAt)
}

func (c *Client) saveHumanRead(ctx context.Context, state *RoomState, readAt time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !readAt.After(state.HumanRead) {
		return nil
	}
	previous := state.HumanRead
	state.HumanRead = readAt
	if err := c.login.Save(ctx); err != nil {
		state.HumanRead = previous
		return err
	}
	return nil
}

func (c *Client) HandleMatrixReadReceipt(ctx context.Context, msg *bridgev2.MatrixReadReceipt) error {
	if msg.Receipt.ThreadID != "" && msg.Receipt.ThreadID != event.ReadReceiptThreadMain {
		return nil
	}
	c.mu.RLock()
	api, generation := c.api, c.runContext
	state := c.meta.Rooms[string(msg.Portal.ID)]
	c.mu.RUnlock()
	if state == nil || msg.Portal.Receiver != c.login.ID {
		return errors.New("refusing read receipt in an unverified Dot room")
	}
	target := msg.ExactMessage
	if target == nil {
		var err error
		if target, err = c.connector.bridge.DB.Message.GetLastNonFakePartAtOrBeforeTime(ctx, msg.Portal.PortalKey, msg.ReadUpTo); err != nil {
			return err
		}
	}
	c.mu.RLock()
	read := target != nil && target.Timestamp.After(state.HumanRead)
	c.mu.RUnlock()
	if !read {
		return nil
	}
	if api == nil || generation == nil || generation.Err() != nil || !c.IsLoggedIn() {
		return errors.New("not connected to ChatGPT")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(generation, cancel)
	defer stop()
	if err := api.MarkRead(ctx, state.Profile.Room, target.Timestamp); err != nil {
		return err
	}
	msg.ReadUpTo = target.Timestamp
	return c.saveHumanRead(ctx, state, target.Timestamp)
}

var _ bridgev2.ReadReceiptHandlingNetworkAPI = (*Client)(nil)

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
	for _, sender := range []struct {
		member string
		event  bridgev2.EventSender
	}{
		{state.DotMember, bridgev2.EventSender{Sender: c.dotID(state)}},
		{state.HumanMember, bridgev2.EventSender{Sender: c.GetUserID(), SenderLogin: c.login.ID, IsFromMe: true}},
	} {
		if err = c.syncReaction(ctx, state, m, parts, sender.member, sender.event); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) syncReaction(ctx context.Context, state *RoomState, m chatgpt.Message, parts []*database.Message, member string, eventSender bridgev2.EventSender) error {
	partID := textPartID(m, parts)
	if attachments := mappedAttachments(m, parts); len(attachments) > 0 {
		partID = attachments[0].PartID
	}
	emoji := m.Reactions[member]
	sender := eventSender.Sender
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
		user.Reactions = []*bridgev2.BackfillReaction{{Sender: eventSender, Emoji: emoji, TargetPart: &partID}}
	}
	evt := &reactionSnapshot{ReactionSync: simplevent.ReactionSync{EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventReactionSync, PortalKey: c.key(state), Sender: eventSender}, TargetMessage: networkid.MessageID(m.ID), Reactions: &bridgev2.ReactionSyncData{Users: map[networkid.UserID]*bridgev2.ReactionSyncUser{sender: user}}}, part: partID}
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
