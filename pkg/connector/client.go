package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"github.com/google/uuid"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
)

type Client struct {
	connector      *Connector
	login          *bridgev2.UserLogin
	meta           *LoginMetadata
	lifecycle      sync.Mutex
	sendMu         sync.Mutex
	mu             sync.RWMutex
	api            *chatgpt.Client
	runContext     context.Context
	cancel         context.CancelFunc
	done           chan struct{}
	connected      atomic.Bool
	lastFullScan   time.Time
	threadMessages map[string]*threadMessage
	signalWake     chan struct{}
	rescan         atomic.Bool
	receipts       map[string]*receiptState
}

func (c *Client) Connect(ctx context.Context) {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.cancel != nil {
		return
	}
	ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Lock()
	c.runContext = ctx
	c.mu.Unlock()
	c.done = make(chan struct{})
	go c.run(ctx)
}
func (c *Client) Disconnect() {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	c.mu.Lock()
	c.connected.Store(false)
	c.api = nil
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.Unlock()
	if c.cancel != nil {
		<-c.done
		c.cancel = nil
	}
	c.sendMu.Lock()
	c.sendMu.Unlock()
	c.mu.Lock()
	c.connected.Store(false)
	c.api = nil
	c.mu.Unlock()
}
func (c *Client) IsLoggedIn() bool { return c.connected.Load() }
func (c *Client) LogoutRemote(context.Context) {
	c.connector.loginMu.Lock()
	defer c.connector.loginMu.Unlock()
	if c.connector.loggingOut == nil {
		c.connector.loggingOut = make(map[networkid.UserLoginID]bool)
	}
	c.connector.loggingOut[c.login.ID] = true
	if current, ok := c.login.Client.(*Client); ok && current != c {
		current.Disconnect()
	}
	c.Disconnect()
	ctx, cancel := context.WithTimeout(c.connector.bridge.BackgroundCtx, 30*time.Second)
	defer cancel()
	if err := c.connector.removeCredentials(ctx, c.login); err != nil {
		c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateUnknownError, Message: err.Error()})
	}
}

func (c *Client) state(err error) {
	c.connected.Store(false)
	state := status.StateTransientDisconnect
	var httpErr *chatgpt.HTTPError
	if errors.As(err, &httpErr) && (httpErr.Status == 401 || httpErr.Status == 403) {
		state = status.StateBadCredentials
	}
	c.login.BridgeState.Send(status.BridgeState{StateEvent: state, Message: err.Error()})
}
func (c *Client) run(ctx context.Context) {
	defer close(c.done)
	c.lastFullScan = time.Time{}
	c.signalWake = make(chan struct{}, 1)
	c.receipts = make(map[string]*receiptState)
	for _, watch := range c.threadMessages {
		watch.NextRefresh = time.Time{}
	}
	c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnecting})
	creds, err := c.connector.readCredentials(ctx, c.login)
	if err != nil {
		c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateBadCredentials, Message: err.Error()})
		return
	}
	api, err := chatgpt.New(creds)
	if err != nil {
		c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateBadCredentials, Message: err.Error()})
		return
	}
	if api.Identity.Account != c.meta.Account || api.Identity.User != c.meta.User {
		c.state(errors.New("credential identity changed; reconnect the original ChatGPT account"))
		return
	}
	api.PersistCredentials = func(updated chatgpt.Credentials) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return c.connector.writeCredentials(ctx, c.login, updated)
	}
	delay := time.Second * 3
	for ctx.Err() == nil {
		err = c.verify(ctx, api)
		if err == nil {
			break
		}
		c.state(err)
		if !wait(ctx, delay) {
			return
		}
		delay = min(delay*2, time.Minute)
	}
	if ctx.Err() != nil {
		return
	}
	c.mu.Lock()
	c.api = api
	c.mu.Unlock()
	streamCtx, stopStream := context.WithCancel(ctx)
	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		c.streamSignals(streamCtx, api)
	}()
	defer func() { stopStream(); <-streamDone }()
	delay = 3 * time.Second
	nextProfileRefresh := time.Now().Add(time.Minute)
	for ctx.Err() == nil {
		err = c.poll(ctx, api)
		threadErr := c.refreshThreads(ctx, api)
		if err == nil {
			err = threadErr
		}
		if !time.Now().Before(nextProfileRefresh) {
			if profileErr := c.verify(ctx, api); profileErr != nil {
				c.login.Log.Warn().Err(profileErr).Msg("Could not refresh ChatGPT profiles")
			}
			nextProfileRefresh = time.Now().Add(time.Minute)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.state(err)
			delay = min(delay*2, time.Minute)
		} else {
			c.connected.Store(true)
			c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
			delay = 3 * time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-c.signalWake:
			timer.Stop()
		case <-timer.C:
		}
	}
}
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (c *Client) verify(ctx context.Context, api *chatgpt.Client) error {
	for _, state := range c.meta.Rooms {
		r, err := api.Verify(ctx, state.Profile)
		if err != nil {
			return err
		}
		current, err := roomState(api, state.Profile, r)
		if err != nil {
			return err
		}
		if current.DotMember != state.DotMember || current.HumanMember != state.HumanMember {
			return errors.New("Dot room members changed; refusing to change existing identity mappings")
		}
		selfData, selfErr := api.SelfAvatar(ctx, state.Profile)
		selfHash := ""
		if len(selfData) > 0 {
			hash := sha256.Sum256(selfData)
			selfHash = hex.EncodeToString(hash[:])
		}
		c.mu.RLock()
		selfMXC, selfFile := c.login.RemoteProfile.Avatar, c.login.RemoteProfile.AvatarFile
		previousHash := c.meta.SelfAvatarHash
		c.mu.RUnlock()
		if selfErr == nil && len(selfData) > 0 && (selfHash != previousHash || selfMXC == "") {
			selfMXC, selfFile, selfErr = c.connector.bridge.Bot.UploadMedia(ctx, "", selfData, "avatar", http.DetectContentType(selfData))
		} else if selfErr == nil && len(selfData) == 0 {
			selfMXC, selfFile = "", nil
		}
		if selfErr != nil {
			c.login.Log.Warn().Err(selfErr).Msg("Could not update own account avatar")
		}
		c.mu.Lock()
		state.HumanName = current.HumanName
		c.login.RemoteProfile.Name = current.HumanName
		if api.Identity.Email != "" {
			c.login.RemoteName = api.Identity.Email
			c.login.RemoteProfile.Email = api.Identity.Email
		}
		if selfErr == nil {
			c.login.RemoteProfile.Avatar = selfMXC
			c.login.RemoteProfile.AvatarFile = selfFile
			c.meta.SelfAvatarHash = selfHash
		}
		if err = c.login.Save(ctx); err != nil {
			c.mu.Unlock()
			return err
		}
		c.mu.Unlock()
		ghost, err := c.connector.bridge.GetGhostByID(ctx, c.GetUserID())
		if err != nil {
			return err
		}
		selfInfo, err := c.GetUserInfo(ctx, ghost)
		if err != nil {
			return err
		}
		ghost.UpdateInfo(ctx, selfInfo)
		avatarData, avatarErr := api.Avatar(ctx, state.Profile)
		if avatarErr != nil {
			c.login.Log.Warn().Err(avatarErr).Msg("Could not update Dot avatar")
		} else if len(avatarData) > 0 {
			hash := sha256.Sum256(avatarData)
			c.mu.Lock()
			state.Avatar = &bridgev2.Avatar{ID: networkid.AvatarID(hex.EncodeToString(hash[:])), Get: func(context.Context) ([]byte, error) { return avatarData, nil }}
			c.mu.Unlock()
		} else {
			c.mu.Lock()
			state.Avatar = &bridgev2.Avatar{Remove: true}
			c.mu.Unlock()
		}
		portal, err := c.connector.bridge.GetPortalByKey(ctx, c.key(state))
		if err != nil {
			return err
		}
		if portal.MXID == "" {
			if err = portal.CreateMatrixRoom(ctx, c.login, c.chatInfo(state)); err != nil {
				return err
			}
		} else {
			portal.UpdateInfo(ctx, c.chatInfo(state), c.login, nil, time.Time{})
		}
	}
	return nil
}
func (c *Client) key(s *RoomState) networkid.PortalKey {
	return networkid.PortalKey{ID: networkid.PortalID(s.Profile.Room), Receiver: c.login.ID}
}
func (c *Client) dotID(s *RoomState) networkid.UserID {
	return networkid.UserID("dot:" + c.meta.Account + ":" + s.Profile.ID)
}
func (c *Client) GetUserID() networkid.UserID {
	return networkid.UserID("human:" + c.meta.Account + ":" + c.meta.User)
}
func (c *Client) IsThisUser(_ context.Context, id networkid.UserID) bool { return id == c.GetUserID() }
func (c *Client) chatInfo(s *RoomState) *bridgev2.ChatInfo {
	dot := c.dotID(s)
	return &bridgev2.ChatInfo{Name: ptr.Ptr(s.Profile.Name), Avatar: s.Avatar, Type: ptr.Ptr(database.RoomTypeDM), Members: &bridgev2.ChatMemberList{IsFull: true, TotalMemberCount: 2, OtherUserID: dot, Members: []bridgev2.ChatMember{{EventSender: bridgev2.EventSender{Sender: dot}, Membership: event.MembershipJoin, UserInfo: &bridgev2.UserInfo{Name: ptr.Ptr(s.Profile.Name), Avatar: s.Avatar, IsBot: ptr.Ptr(true)}}, {EventSender: bridgev2.EventSender{Sender: c.GetUserID(), SenderLogin: c.login.ID, IsFromMe: true}, Membership: event.MembershipJoin}}}}
}
func (c *Client) GetChatInfo(_ context.Context, p *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := c.meta.Rooms[string(p.ID)]
	if s == nil || p.Receiver != c.login.ID {
		return nil, errors.New("unknown Dot room")
	}
	return c.chatInfo(s), nil
}
func (c *Client) GetUserInfo(_ context.Context, g *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.meta.Rooms {
		if g.ID == c.GetUserID() {
			name := s.HumanName
			if name == "" {
				name = "ChatGPT account"
			}
			return &bridgev2.UserInfo{Name: ptr.Ptr(name)}, nil
		}
		if c.dotID(s) == g.ID {
			return &bridgev2.UserInfo{Name: ptr.Ptr(s.Profile.Name), Avatar: s.Avatar, IsBot: ptr.Ptr(true)}, nil
		}
	}
	return nil, errors.New("unknown Dot contact")
}
func (c *Client) GetCapabilities(context.Context, *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{ID: "chatgpt-dots.signals.v1", ReadReceipts: true, TypingNotifications: true, File: event.FileFeatureMap{
		"m.image":         {MimeTypes: map[string]event.CapabilitySupportLevel{"image/*": event.CapLevelFullySupported}, Caption: event.CapLevelFullySupported, MaxSize: chatgpt.MaxUploadSize},
		"m.file":          {MimeTypes: map[string]event.CapabilitySupportLevel{"*/*": event.CapLevelFullySupported}, Caption: event.CapLevelFullySupported, MaxSize: chatgpt.MaxUploadSize},
		"m.audio":         {MimeTypes: map[string]event.CapabilitySupportLevel{"*/*": event.CapLevelRejected}},
		"m.video":         {MimeTypes: map[string]event.CapabilitySupportLevel{"video/*": event.CapLevelFullySupported}, Caption: event.CapLevelFullySupported, MaxSize: chatgpt.MaxUploadSize},
		event.CapMsgVoice: {MimeTypes: map[string]event.CapabilitySupportLevel{"*/*": event.CapLevelRejected}},
	}, Reply: event.CapLevelRejected, Thread: event.CapLevelRejected, Edit: event.CapLevelRejected, Delete: event.CapLevelRejected, Reaction: event.CapLevelRejected, Poll: event.CapLevelRejected, LocationMessage: event.CapLevelRejected}
}
func revision(m chatgpt.Message) string {
	return messageRevision(m, false)
}
func messageRevision(m chatgpt.Message, includeUpdateTime bool) string {
	type attachmentRevision struct {
		Type         string `json:"type"`
		FileID       string `json:"file_id"`
		AttachmentID string `json:"attachment_id"`
	}
	attachments := make([]attachmentRevision, 0, len(m.Content.Attachments))
	for _, raw := range m.Content.Attachments {
		var attachment attachmentRevision
		_ = json.Unmarshal(raw, &attachment)
		if attachment.Type != "link" || includeUpdateTime {
			attachments = append(attachments, attachment)
		}
	}
	encoded, _ := json.Marshal(attachments)
	updated := ""
	if includeUpdateTime {
		updated = m.Updated.Format(time.RFC3339Nano)
	}
	hash := sha256.Sum256([]byte(updated + "\x00" + m.Content.Text + fmt.Sprint(m.Deleted) + "\x00" + string(encoded)))
	return hex.EncodeToString(hash[:])
}
func (c *Client) poll(ctx context.Context, api *chatgpt.Client) error {
	if c.rescan.Swap(false) {
		c.lastFullScan = time.Time{}
	}
	full := time.Since(c.lastFullScan) >= time.Minute
	for _, state := range c.meta.Rooms {
		receipt, receiptChanged, err := c.readReceipt(ctx, api, state)
		if err != nil {
			return err
		}
		if receiptChanged {
			c.lastFullScan = time.Time{}
			full = true
		}
		c.mu.RLock()
		var pendingIDs []string
		for requestID, pending := range c.meta.Pending {
			if pending.Room == state.Profile.Room && !pending.KnownUnsent {
				pendingIDs = append(pendingIDs, requestID)
			}
		}
		c.mu.RUnlock()
		for _, requestID := range pendingIDs {
			m, err := c.findRequest(ctx, api, state, requestID)
			if err != nil {
				return err
			}
			if m != nil {
				if err = c.deliver(ctx, state, *m); err != nil {
					return err
				}
			}
		}
		after := state.Cursor
		if len(state.Overlap) > 0 {
			after = state.Overlap[0]
		}
		if full {
			after = state.Baseline
		}
		seen := map[string]bool{}
		for {
			if seen[after] {
				return errors.New("provider pagination repeated; checkpoint retained")
			}
			seen[after] = true
			page, err := api.Messages(ctx, state.Profile.Room, after)
			if err != nil {
				return err
			}
			sort.SliceStable(page.Items, func(i, j int) bool { return page.Items[i].Created.Before(page.Items[j].Created) })
			for _, m := range page.Items {
				if m.ID == "" || m.Created.IsZero() {
					return errors.New("provider message missing stable identity or timestamp")
				}
				if m.ID == state.Baseline || m.Created.Before(state.Linked) {
					continue
				}
				if err = c.deliver(ctx, state, m); err != nil {
					return err
				}
				if err = c.syncReactions(ctx, state, m); err != nil {
					return err
				}
				if m.Sender == state.HumanMember && m.Deleted == nil && !m.Created.After(receipt.Native.ReadAt) && m.Created.After(receipt.TargetTime) {
					receipt.Target, receipt.TargetTime = networkid.MessageID(m.ID), m.Created
				}
				c.mu.Lock()
				oldCursor, oldOverlap := state.Cursor, append([]string(nil), state.Overlap...)
				if state.Cursor != m.ID {
					exists := false
					for _, id := range state.Overlap {
						if id == m.ID {
							exists = true
						}
					}
					if !exists {
						state.Overlap = append(state.Overlap, m.ID)
						if len(state.Overlap) > 33 {
							state.Overlap = state.Overlap[len(state.Overlap)-33:]
						}
						state.Cursor = m.ID
					}
				}
				err = c.login.Save(ctx)
				if err != nil {
					state.Cursor = oldCursor
					state.Overlap = oldOverlap
				}
				c.mu.Unlock()
				if err != nil {
					return err
				}
			}
			if len(page.Items) == 0 || (len(page.Items) < 32 && page.Next == "") {
				break
			}
			next := page.Items[len(page.Items)-1].ID
			if page.Next != "" {
				next = page.Next
			}
			if next == after {
				return errors.New("provider pagination did not advance")
			}
			after = next
		}
		if err := c.sendReceipt(ctx, state, receipt); err != nil {
			return err
		}
	}
	if full {
		c.lastFullScan = time.Now()
	}
	return nil
}
func (c *Client) deliver(ctx context.Context, state *RoomState, m chatgpt.Message) error {
	if m.ID == state.Baseline || m.Created.Before(state.Linked) {
		return nil
	}
	sender := bridgev2.EventSender{}
	if m.Sender == state.DotMember {
		sender.Sender = c.dotID(state)
	} else if m.Sender == state.HumanMember {
		sender = bridgev2.EventSender{Sender: c.GetUserID(), SenderLogin: c.login.ID, IsFromMe: true}
	} else {
		return errors.New("message from an unverified Dot room member")
	}
	if err := c.prepareThreads(ctx, state, &m); err != nil {
		return err
	}
	done := make(chan struct{}, 1)
	meta := simplevent.EventMeta{Type: bridgev2.RemoteEventMessageUpsert, PortalKey: c.key(state), Sender: sender, Timestamp: m.Created, PostHandleFunc: func(context.Context, *bridgev2.Portal) {
		select {
		case done <- struct{}{}:
		default:
		}
	}}
	msg := &simplevent.Message[chatgpt.Message]{EventMeta: meta, ID: networkid.MessageID(m.ID), TransactionID: networkid.TransactionID(m.RequestID), TargetMessage: networkid.MessageID(m.ID), Data: m}
	msg.MutateContextFunc = func(context.Context) context.Context { return ctx }
	msg.PreHandleFunc = func(ctx context.Context, p *bridgev2.Portal) {
		c.mu.RLock()
		pending, ok := c.meta.Pending[m.RequestID]
		c.mu.RUnlock()
		if !ok || pending.Room != state.Profile.Room || m.Sender != state.HumanMember {
			return
		}
		existing, err := c.connector.bridge.DB.Message.GetFirstPartByID(ctx, c.login.ID, networkid.MessageID(m.ID))
		if err != nil || existing != nil {
			return
		}
		original := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: p, Event: &event.Event{ID: pending.EventID, RoomID: p.MXID, Sender: pending.Sender, Timestamp: pending.Timestamp, Type: event.EventMessage}}}
		original.AddPendingToSave(&database.Message{SenderID: c.GetUserID(), Metadata: &MessageMetadata{Revision: revision(m), OutgoingMedia: pending.Media}}, networkid.TransactionID(m.RequestID), nil)
	}
	msg.ConvertMessageFunc = func(ctx context.Context, p *bridgev2.Portal, intent bridgev2.MatrixAPI, _ chatgpt.Message) (*bridgev2.ConvertedMessage, error) {
		return c.convertIncoming(ctx, state, p, intent, m, nil)
	}
	msg.ConvertEditFunc = func(ctx context.Context, p *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, _ chatgpt.Message) (*bridgev2.ConvertedEdit, error) {
		converted, err := c.convertIncoming(ctx, state, p, intent, m, existing)
		if err != nil {
			return nil, err
		}
		result := &bridgev2.ConvertedEdit{AddedParts: &bridgev2.ConvertedMessage{}}
		byID := make(map[networkid.PartID]*database.Message, len(existing))
		for _, part := range existing {
			byID[part.PartID] = part
		}
		for _, part := range converted.Parts {
			if old := byID[part.ID]; old != nil {
				edit := part.ToEditPart(old)
				if md, ok := part.DBMetadata.(*MessageMetadata); ok && md.Thread != nil {
					edit.TopLevelExtra = map[string]any{"com.beeper.dont_render_edited": true}
				}
				result.ModifiedParts = append(result.ModifiedParts, edit)
			} else {
				result.AddedParts.Parts = append(result.AddedParts.Parts, part)
			}
		}
		expected := expectedIncomingParts(m, existing)
		for _, old := range existing {
			if !expected[old.PartID] {
				// The pinned SDK deletes the mapping even when redaction fails.
				// Keep it until both operations succeed so the next poll can retry.
				_, err = intent.SendMessage(ctx, p.MXID, event.EventRedaction, &event.Content{Parsed: &event.RedactionEventContent{Redacts: old.MXID}}, &bridgev2.MatrixSendExtra{Timestamp: m.Updated})
				if err != nil {
					return nil, errors.New("Matrix attachment redaction failed; checkpoint retained")
				}
				if err = c.connector.bridge.DB.Message.Delete(ctx, old.RowID); err != nil {
					return nil, err
				}
			}
		}
		return result, nil
	}
	msg.HandleExistingFunc = func(ctx context.Context, _ *bridgev2.Portal, _ bridgev2.MatrixAPI, existing []*database.Message, _ chatgpt.Message) (bridgev2.UpsertResult, error) {
		legacy := incomingRevisionsWithBase(m, existing, messageRevision(m, true))
		current := incomingRevisions(m, existing)
		for _, part := range existing {
			md, ok := part.Metadata.(*MessageMetadata)
			if ok && len(messageAttachments(m)) == len(mappedAttachments(m, existing)) && md.Revision == legacy[part.PartID] && md.Revision != current[part.PartID] {
				previous := md.Revision
				md.Revision = current[part.PartID]
				if err := c.connector.bridge.DB.Message.Update(ctx, part); err != nil {
					md.Revision = previous
					return bridgev2.UpsertResult{}, err
				}
			}
		}
		if incomingSaved(m, existing) {
			return bridgev2.UpsertResult{}, nil
		}
		edit := *msg
		edit.Type = bridgev2.RemoteEventEdit
		edit.PostHandleFunc = nil
		if !m.Updated.IsZero() {
			edit.Timestamp = m.Updated
		}
		for _, thread := range m.Threads {
			if thread != nil && time.Unix(thread.UpdatedAt, 0).After(edit.Timestamp) {
				edit.Timestamp = time.Unix(thread.UpdatedAt, 0)
			}
		}
		return bridgev2.UpsertResult{SubEvents: []bridgev2.RemoteEvent{&edit}}, nil
	}
	result := c.login.QueueRemoteEvent(msg)
	if !result.Success || result.Ignored {
		return errors.New("bridge could not queue remote message; checkpoint retained")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	saved, err := c.connector.bridge.DB.Message.GetAllPartsByID(ctx, c.login.ID, networkid.MessageID(m.ID))
	if err != nil {
		return err
	}
	if !incomingSaved(m, saved) {
		return errors.New("remote message parts not durably saved; checkpoint retained")
	}
	if err = c.saveThreadSnapshots(ctx, m, saved); err != nil {
		return err
	}
	c.mu.Lock()
	pending, exists := c.meta.Pending[m.RequestID]
	if exists {
		delete(c.meta.Pending, m.RequestID)
		err = c.login.Save(ctx)
		if err != nil {
			c.meta.Pending[m.RequestID] = pending
		}
	}
	c.mu.Unlock()
	if err != nil {
		return err
	}
	return nil
}
func (c *Client) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	media := msg.Content.MsgType == event.MsgImage || msg.Content.MsgType == event.MsgFile || msg.Content.MsgType == event.MsgVideo
	if !media && (msg.Content.MsgType != event.MsgText || msg.Content.Body == "") {
		return nil, errors.New("only plain text, images, files and videos are supported; audio and voice messages are not supported")
	}
	if msg.ReplyTo != nil || msg.ThreadRoot != nil {
		return nil, errors.New("replies and threads are not supported; send a standalone message")
	}
	c.mu.RLock()
	api := c.api
	runContext := c.runContext
	state := c.meta.Rooms[string(msg.Portal.ID)]
	c.mu.RUnlock()
	if api == nil || runContext == nil || runContext.Err() != nil || !c.connected.Load() {
		return nil, errors.New("ChatGPT Dots is disconnected; reconnect before sending")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(runContext, cancel)
	defer stop()
	if state == nil || msg.Portal.Receiver != c.login.ID {
		return nil, errors.New("refusing unverified Dot room")
	}
	room, err := api.Verify(ctx, state.Profile)
	if err != nil {
		c.state(err)
		return nil, err
	}
	current, err := roomState(api, state.Profile, room)
	if err != nil {
		return nil, err
	}
	if current.DotMember != state.DotMember || current.HumanMember != state.HumanMember {
		return nil, errors.New("Dot room members changed; refusing send")
	}
	requestID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(string(c.login.ID)+"\x00"+string(msg.Event.ID))).String()
	fingerprint := []byte(msg.Content.Body)
	text := msg.Content.Body
	if media {
		text = msg.Content.GetCaption()
		fingerprint, err = json.Marshal(msg.Content)
		if err != nil {
			return nil, errors.New("cannot fingerprint attachment content")
		}
		if msg.Content.Info != nil && msg.Content.Info.Size > chatgpt.MaxUploadSize {
			return nil, errors.New("attachment exceeds the bridge-local 20 MiB limit")
		}
	}
	hash := sha256.Sum256(fingerprint)
	bodyHash := hex.EncodeToString(hash[:])
	c.mu.Lock()
	if c.meta.Pending == nil {
		c.meta.Pending = map[string]PendingSend{}
	}
	pending, retry := c.meta.Pending[requestID]
	wasUncertain := retry && !pending.KnownUnsent
	if retry && (pending.BodyHash != bodyHash || pending.Room != state.Profile.Room || pending.EventID != msg.Event.ID || pending.Media != media) {
		c.mu.Unlock()
		return nil, errors.New("retry content differs from the original request; refusing to reuse its request ID")
	}
	if !retry {
		pending = PendingSend{Room: state.Profile.Room, EventID: msg.Event.ID, Sender: msg.Event.Sender, Timestamp: msg.Event.Timestamp, BodyHash: bodyHash, Media: media, KnownUnsent: true}
	}
	c.meta.Pending[requestID] = pending
	err = c.login.Save(ctx)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if retry && !pending.KnownUnsent {
		found, err := c.findRequest(ctx, api, state, requestID)
		if err != nil {
			return nil, err
		}
		if found != nil {
			return c.sendResponse(*found, media, runContext), nil
		}
	}
	var attachments []chatgpt.FileReference
	if media {
		if pending.FileID == "" {
			data, err := c.downloadAttachment(ctx, msg.Content)
			if err != nil {
				return nil, err
			}
			mimeType := ""
			if msg.Content.Info != nil {
				mimeType = msg.Content.Info.MimeType
			}
			pending.FileID, err = api.Upload(ctx, state.Profile.Room, msg.Content.GetFileName(), mimeType, data)
			if err != nil {
				return nil, fmt.Errorf("attachment upload failed; no message sent (an uncertain upload may leave an unused file): %w", err)
			}
		}
		c.mu.Lock()
		c.meta.Pending[requestID] = pending
		err = c.login.Save(ctx)
		c.mu.Unlock()
		if err != nil {
			return nil, errors.New("could not persist attachment identity; no message sent")
		}
		attachments = []chatgpt.FileReference{{Type: "file", FileID: pending.FileID}}
	}
	pending.KnownUnsent = false
	c.mu.Lock()
	c.meta.Pending[requestID] = pending
	err = c.login.Save(ctx)
	if err != nil {
		pending.KnownUnsent = !wasUncertain
		c.meta.Pending[requestID] = pending
	}
	c.mu.Unlock()
	if err != nil {
		return nil, errors.New("could not persist send intent; no message sent")
	}
	m, err := api.Send(ctx, state.Profile.Room, text, requestID, attachments)
	if err != nil {
		certain := false
		var httpErr *chatgpt.HTTPError
		if errors.As(err, &httpErr) {
			certain = !wasUncertain && httpErr.Status >= 400 && httpErr.Status < 500 && httpErr.Status != 408
			if httpErr.Status == 401 || httpErr.Status == 403 {
				c.state(err)
			}
		}
		if certain {
			c.mu.Lock()
			pending.KnownUnsent = true
			c.meta.Pending[requestID] = pending
			if saveErr := c.login.Save(ctx); saveErr != nil {
				err = fmt.Errorf("%w; could not persist rejection state", err)
			}
			c.mu.Unlock()
		}
		if !certain {
			err = fmt.Errorf("%w; delivery unknown: retry the original Beeper event to reconcile and reuse its request ID, not a new message", err)
		}
		return nil, bridgev2.WrapErrorInStatus(err).WithIsCertain(certain).WithErrorAsMessage()
	}
	if m.Sender != state.HumanMember {
		return nil, errors.New("send response sender identity mismatch; delivery unknown")
	}
	return c.sendResponse(*m, media, runContext), nil
}

func (c *Client) sendResponse(m chatgpt.Message, media bool, generation context.Context) *bridgev2.MatrixMessageResponse {
	return &bridgev2.MatrixMessageResponse{DB: &database.Message{ID: networkid.MessageID(m.ID), SenderID: c.GetUserID(), Timestamp: m.Created, Metadata: &MessageMetadata{Revision: revision(m), OutgoingMedia: media}}, PostSave: func(ctx context.Context, _ *database.Message) {
		c.mu.Lock()
		defer c.mu.Unlock()
		// The framework calls PostSave after this client's handler has returned.
		if generation.Err() != nil || c.runContext != generation {
			return
		}
		pending, ok := c.meta.Pending[m.RequestID]
		if !ok {
			return
		}
		delete(c.meta.Pending, m.RequestID)
		if c.login.Save(ctx) != nil {
			c.meta.Pending[m.RequestID] = pending
		}
	}}
}
func (c *Client) findRequest(ctx context.Context, api *chatgpt.Client, state *RoomState, requestID string) (*chatgpt.Message, error) {
	after := state.Baseline
	seen := map[string]bool{}
	for !seen[after] {
		seen[after] = true
		page, err := api.Messages(ctx, state.Profile.Room, after)
		if err != nil {
			return nil, err
		}
		for _, m := range page.Items {
			if m.RequestID == requestID && m.Sender == state.HumanMember {
				return &m, nil
			}
		}
		if len(page.Items) == 0 || (len(page.Items) < 32 && page.Next == "") {
			return nil, nil
		}
		sort.SliceStable(page.Items, func(i, j int) bool { return page.Items[i].Created.Before(page.Items[j].Created) })
		after = page.Items[len(page.Items)-1].ID
		if page.Next != "" {
			after = page.Next
		}
	}
	return nil, errors.New("cannot reconcile uncertain send: provider pagination repeated; refusing another POST")
}

var _ bridgev2.NetworkAPI = (*Client)(nil)
