package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
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
	"maunium.net/go/mautrix/id"
)

const (
	pollInterval          = 3 * time.Second
	livePollInterval      = 30 * time.Second
	avatarRefreshInterval = 30 * time.Minute
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
	threadMessages map[string]*threadMessage
	signalWake     chan struct{}
	signalsLive    atomic.Bool
	rescan         atomic.Bool
	receipts       map[string]*receiptState
	delivered      map[string][32]byte
	deliveredOrder []string
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
		c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateUnknownError, Error: stateCredentialCleanupFailed, Info: map[string]any{"go_error": err.Error()}})
	}
}

func (c *Client) state(err error) bool {
	c.connected.Store(false)
	state := status.BridgeState{StateEvent: status.StateTransientDisconnect, Error: stateSyncFailed, Info: map[string]any{"go_error": err.Error()}}
	var httpErr *chatgpt.HTTPError
	if errors.Is(err, errDotUnavailable) {
		state = badCredentialsState(stateDotUnavailable, err)
	} else if errors.As(err, &httpErr) {
		c.login.Log.Warn().Err(err).
			Int("status", httpErr.Status).
			Str("path", httpErr.Path).
			Bool("cloudflare_challenge", httpErr.Challenge).
			Msg("ChatGPT request failed")
		switch {
		case httpErr.Status == http.StatusUnauthorized:
			state = badCredentialsState(stateAuthExpired, err)
		case httpErr.Challenge:
			state.Error = stateCloudflareChallenge
		case httpErr.Status == http.StatusForbidden:
			state.Error = stateForbidden
		case httpErr.Status == http.StatusTooManyRequests:
			state.Error = stateRateLimited
		case httpErr.Status >= 500:
			state.Error = stateServerError
		default:
			state.Error = stateRequestFailed
		}
	} else if isTransportError(err) {
		state.Error = stateNetworkError
	}
	c.login.BridgeState.Send(state)
	return state.StateEvent == status.StateBadCredentials
}
func (c *Client) run(ctx context.Context) {
	defer close(c.done)
	c.signalWake = make(chan struct{}, 1)
	c.receipts = make(map[string]*receiptState)
	c.delivered = make(map[string][32]byte)
	c.deliveredOrder = nil
	for _, watch := range c.threadMessages {
		watch.NextRefresh = time.Time{}
	}
	c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnecting})
	creds, err := c.connector.readCredentials(ctx, c.login)
	if errors.Is(err, errNoCredentials) {
		c.login.BridgeState.Send(badCredentialsState(stateNoCredentials, err))
		return
	} else if err != nil {
		c.login.BridgeState.Send(badCredentialsState(stateBadCredentials, err))
		return
	}
	proxy := &proxyTransport{connector: c.connector, log: c.login.Log}
	api, err := chatgpt.New(creds, proxy)
	if err != nil {
		c.login.BridgeState.Send(badCredentialsState(stateBadCredentials, err))
		return
	}
	if api.Identity.Account != c.meta.Account || api.Identity.User != c.meta.User {
		c.login.BridgeState.Send(badCredentialsState(stateIdentityChanged, errors.New("credential identity changed")))
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
		if err = proxy.update(ctx, "connect"); err == nil {
			err = c.verify(ctx, api, true)
		}
		if err == nil {
			break
		}
		if c.state(err) || !wait(ctx, delay) {
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
	delay = pollInterval
	nextProfileRefresh := time.Now().Add(time.Minute)
	nextAvatarRefresh := time.Now().Add(avatarRefreshInterval)
	var nextPoll time.Time
	for ctx.Err() == nil {
		err = nil
		if !time.Now().Before(nextPoll) {
			err = c.poll(ctx, api)
			nextPoll = time.Now().Add(pollInterval)
			if c.signalsLive.Load() {
				nextPoll = time.Now().Add(livePollInterval)
			}
		}
		err = errors.Join(err, c.refreshThreads(ctx, api))
		if !time.Now().Before(nextProfileRefresh) {
			avatars := !time.Now().Before(nextAvatarRefresh)
			if avatars {
				nextAvatarRefresh = time.Now().Add(avatarRefreshInterval)
			}
			if profileErr := c.verify(ctx, api, avatars); errors.Is(profileErr, errDotUnavailable) {
				c.state(profileErr)
				return
			} else if profileErr != nil {
				c.login.Log.Warn().Err(profileErr).Msg("Could not refresh ChatGPT profiles")
			}
			nextProfileRefresh = time.Now().Add(time.Minute)
		}
		if ctx.Err() != nil {
			return
		}
		if proxy.failed.Load() {
			if proxyErr := proxy.update(ctx, "connect"); proxyErr != nil {
				c.login.Log.Warn().Err(proxyErr).Msg("Could not refresh ChatGPT proxy")
				err = errors.Join(err, proxyErr)
			}
		}
		if err != nil && !onlyMediaRetries(err) {
			if ctx.Err() != nil {
				return
			}
			if c.state(err) {
				return
			}
			delay = min(delay*2, time.Minute)
			nextPoll = time.Time{}
		} else {
			if err != nil {
				c.login.Log.Debug().Err(err).Msg("Waiting to retry a ChatGPT attachment download")
			}
			c.connected.Store(true)
			c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected})
			delay = pollInterval
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-c.signalWake:
			timer.Stop()
			nextPoll = time.Time{}
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
func (c *Client) verify(ctx context.Context, api *chatgpt.Client, avatars bool) error {
	for _, state := range c.meta.Rooms {
		r, err := api.Verify(ctx, state.Profile)
		if err != nil {
			return c.checkDotReplaced(ctx, api, state, err)
		}
		current, err := roomState(api, state.Profile, r)
		if err != nil {
			return err
		}
		if current.DotMember != state.DotMember || current.HumanMember != state.HumanMember {
			return errors.New("Dot room members changed; refusing to change existing identity mappings")
		}
		if avatars {
			c.refreshSelfAvatar(ctx, api, state)
			c.refreshDotAvatar(ctx, api, state)
		}
		c.mu.Lock()
		changed := avatars || state.HumanName != current.HumanName || c.login.RemoteProfile.Name != current.HumanName
		state.HumanName = current.HumanName
		c.login.RemoteProfile.Name = current.HumanName
		if email := api.Identity.Email; email != "" {
			changed = changed || c.login.RemoteName != email || c.login.RemoteProfile.Email != email
			c.login.RemoteName = email
			c.login.RemoteProfile.Email = email
		}
		if changed {
			if err = c.login.Save(ctx); err != nil {
				c.mu.Unlock()
				return err
			}
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
		portal, err := c.connector.bridge.GetPortalByKey(ctx, c.key(state))
		if err != nil {
			return err
		}
		if portal.MXID == "" {
			if err = portal.CreateMatrixRoom(ctx, c.login, c.chatInfo(state)); err != nil {
				return err
			}
			clear(c.delivered)
			c.deliveredOrder = nil
			c.rescan.Store(true)
		} else {
			portal.UpdateInfo(ctx, c.chatInfo(state), c.login, nil, time.Time{})
		}
	}
	return nil
}
func (c *Client) checkDotReplaced(ctx context.Context, api *chatgpt.Client, state *RoomState, err error) error {
	var httpErr *chatgpt.HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusNotFound {
		return err
	}
	profiles, discoverErr := api.Discover(ctx)
	var primaryErr *chatgpt.HTTPError
	missing := errors.Is(discoverErr, chatgpt.ErrNoPrimaryDot) || errors.As(discoverErr, &primaryErr) && primaryErr.PrimaryUnavailable
	replaced := discoverErr == nil && (profiles[0].ID != state.Profile.ID || profiles[0].Room != state.Profile.Room)
	c.login.Log.Warn().
		Bool("primary_dot_missing", missing).
		Bool("primary_dot_replaced", replaced).
		Bool("primary_dot_unknown", discoverErr != nil && !missing).
		Msg("The linked Dot room returned HTTP 404")
	if missing || replaced {
		return fmt.Errorf("%w: %w", errDotUnavailable, err)
	}
	return err
}
func (c *Client) refreshSelfAvatar(ctx context.Context, api *chatgpt.Client, state *RoomState) {
	data, err := api.SelfAvatar(ctx, state.Profile)
	if err != nil {
		c.login.Log.Warn().Err(err).Msg("Could not update own account avatar")
		return
	}
	hash := ""
	if len(data) > 0 {
		sum := sha256.Sum256(data)
		hash = hex.EncodeToString(sum[:])
	}
	c.mu.RLock()
	unchanged := hash == c.meta.SelfAvatarHash && (c.login.RemoteProfile.Avatar != "") == (len(data) > 0)
	c.mu.RUnlock()
	if unchanged {
		return
	}
	var mxc id.ContentURIString
	var file *event.EncryptedFileInfo
	if len(data) > 0 {
		if mxc, file, err = c.connector.bridge.Bot.UploadMedia(ctx, "", data, "avatar", http.DetectContentType(data)); err != nil {
			c.login.Log.Warn().Err(err).Msg("Could not update own account avatar")
			return
		}
	}
	c.mu.Lock()
	c.login.RemoteProfile.Avatar, c.login.RemoteProfile.AvatarFile = mxc, file
	c.meta.SelfAvatarHash = hash
	c.mu.Unlock()
}
func (c *Client) refreshDotAvatar(ctx context.Context, api *chatgpt.Client, state *RoomState) {
	current, data, err := api.Avatar(ctx, state.Profile)
	if current.ID == state.Profile.ID {
		c.mu.Lock()
		state.Profile.Name = current.Name
		c.mu.Unlock()
	}
	if err != nil {
		c.login.Log.Warn().Err(err).Msg("Could not update Dot avatar")
		return
	}
	avatar := &bridgev2.Avatar{Remove: true}
	if len(data) > 0 {
		hash := sha256.Sum256(data)
		avatar = &bridgev2.Avatar{ID: networkid.AvatarID(hex.EncodeToString(hash[:])), Get: func(context.Context) ([]byte, error) { return data, nil }}
	}
	c.mu.Lock()
	state.Avatar = avatar
	c.mu.Unlock()
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
	return &bridgev2.ChatInfo{Name: ptr.Ptr(s.Profile.DisplayName()), Avatar: s.Avatar, Type: ptr.Ptr(database.RoomTypeDM), Members: &bridgev2.ChatMemberList{IsFull: true, TotalMemberCount: 2, OtherUserID: dot, Members: []bridgev2.ChatMember{{EventSender: bridgev2.EventSender{Sender: dot}, Membership: event.MembershipJoin, UserInfo: &bridgev2.UserInfo{Name: ptr.Ptr(s.Profile.DisplayName()), Avatar: s.Avatar, IsBot: ptr.Ptr(true)}}, {EventSender: bridgev2.EventSender{Sender: c.GetUserID(), SenderLogin: c.login.ID, IsFromMe: true}, Membership: event.MembershipJoin}}}}
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
			return &bridgev2.UserInfo{Name: ptr.Ptr(s.Profile.DisplayName()), Avatar: s.Avatar, IsBot: ptr.Ptr(true)}, nil
		}
	}
	return nil, errors.New("unknown Dot contact")
}
func (c *Client) GetCapabilities(context.Context, *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{ID: "chatgpt-dots.replies-reactions.v1", ReadReceipts: true, TypingNotifications: true, ReactionCount: 1, File: event.FileFeatureMap{
		"m.image":         {MimeTypes: map[string]event.CapabilitySupportLevel{"image/*": event.CapLevelFullySupported}, Caption: event.CapLevelFullySupported, MaxSize: chatgpt.MaxUploadSize},
		"m.file":          {MimeTypes: map[string]event.CapabilitySupportLevel{"*/*": event.CapLevelFullySupported}, Caption: event.CapLevelFullySupported, MaxSize: chatgpt.MaxUploadSize},
		"m.audio":         {MimeTypes: map[string]event.CapabilitySupportLevel{"*/*": event.CapLevelRejected}},
		"m.video":         {MimeTypes: map[string]event.CapabilitySupportLevel{"video/*": event.CapLevelFullySupported}, Caption: event.CapLevelFullySupported, MaxSize: chatgpt.MaxUploadSize},
		event.CapMsgVoice: {MimeTypes: map[string]event.CapabilitySupportLevel{"*/*": event.CapLevelRejected}},
	}, Reply: event.CapLevelFullySupported, Thread: event.CapLevelFullySupported, Edit: event.CapLevelRejected, Delete: event.CapLevelRejected, Reaction: event.CapLevelFullySupported, Poll: event.CapLevelRejected, LocationMessage: event.CapLevelRejected}
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
	if m.Content.Elicitation != nil {
		confirmation, _ := json.Marshal(m.Content.Elicitation)
		encoded = append(append(encoded, '\x00'), confirmation...)
		encoded = append(encoded, byte(confirmationRenderingVersion))
	}
	updated := ""
	if includeUpdateTime {
		updated = m.Updated.Format(time.RFC3339Nano)
	}
	hash := sha256.Sum256([]byte(updated + "\x00" + m.Content.Text + fmt.Sprint(m.Deleted) + "\x00" + string(encoded)))
	return hex.EncodeToString(hash[:])
}
func messageFingerprint(m chatgpt.Message) ([32]byte, bool) {
	data, err := json.Marshal(m)
	return sha256.Sum256(data), err == nil
}
func (c *Client) advanceCursor(ctx context.Context, state *RoomState, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if state.Cursor == id || slices.Contains(state.Overlap, id) {
		return nil
	}
	oldCursor, oldOverlap := state.Cursor, state.Overlap
	state.Overlap = append(slices.Clone(state.Overlap), id)
	if len(state.Overlap) > 33 {
		state.Overlap = state.Overlap[len(state.Overlap)-33:]
	}
	state.Cursor = id
	if err := c.login.Save(ctx); err != nil {
		state.Cursor, state.Overlap = oldCursor, oldOverlap
		return err
	}
	return nil
}
func (c *Client) poll(ctx context.Context, api *chatgpt.Client) (err error) {
	full := c.rescan.Swap(false) || c.meta.Rescan || !c.meta.ConfirmationsSynced || c.meta.ConfirmationVersion != confirmationRenderingVersion
	if full {
		if !c.meta.Rescan {
			c.mu.Lock()
			c.meta.Rescan = true
			err = c.login.Save(ctx)
			if err != nil {
				c.meta.Rescan = false
			}
			c.mu.Unlock()
			if err != nil {
				c.rescan.Store(true)
				return err
			}
		}
		defer func() {
			if err != nil && !onlyMediaRetries(err) {
				c.rescan.Store(true)
			}
		}()
	}
	var pending error
	for _, state := range c.meta.Rooms {
		receipt, err := c.readReceipt(ctx, api, state)
		if err != nil {
			return err
		}
		for id, retry := range state.Media {
			if time.Now().Before(retry.Next) {
				continue
			}
			page, err := api.Messages(ctx, state.Profile.Room, retry.After)
			if err != nil {
				return err
			}
			var m *chatgpt.Message
			for i := range page.Items {
				if page.Items[i].ID == id {
					m = &page.Items[i]
					break
				}
			}
			if m == nil {
				next := retry.Start
				if page.Next != "" {
					next = page.Next
				} else if len(page.Items) >= 32 {
					sort.SliceStable(page.Items, func(i, j int) bool { return page.Items[i].Created.Before(page.Items[j].Created) })
					next = page.Items[len(page.Items)-1].ID
				}
				if err = c.saveMediaRetry(ctx, state, id, next, retry.Revision, true); err != nil {
					return err
				}
				continue
			}
			if m.Created.IsZero() {
				return errors.New("provider message missing timestamp")
			}
			if err = c.deliverMedia(ctx, state, *m, retry.After); err != nil {
				if !onlyMediaRetries(err) {
					return err
				}
				pending = errors.Join(pending, err)
			}
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
				if err = c.deliverMedia(ctx, state, *m, state.Baseline); err != nil {
					if !onlyMediaRetries(err) {
						return err
					}
					pending = errors.Join(pending, err)
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
				fingerprint, ok := messageFingerprint(m)
				if full || !ok || c.delivered[m.ID] != fingerprint {
					if err = c.deliverMedia(ctx, state, m, after); err != nil {
						if !onlyMediaRetries(err) {
							return err
						}
						pending = errors.Join(pending, err)
						if !full {
							if err = c.advanceCursor(ctx, state, m.ID); err != nil {
								return err
							}
						}
						continue
					}
					if ok {
						if _, exists := c.delivered[m.ID]; !exists {
							if len(c.deliveredOrder) == 33 {
								delete(c.delivered, c.deliveredOrder[0])
								c.deliveredOrder = c.deliveredOrder[1:]
							}
							c.deliveredOrder = append(c.deliveredOrder, m.ID)
						}
						c.delivered[m.ID] = fingerprint
					}
				}
				if !full {
					if err = c.advanceCursor(ctx, state, m.ID); err != nil {
						return err
					}
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
		c.mu.Lock()
		oldSynced, oldVersion := c.meta.ConfirmationsSynced, c.meta.ConfirmationVersion
		c.meta.ConfirmationsSynced = true
		c.meta.Rescan = false
		c.meta.ConfirmationVersion = confirmationRenderingVersion
		err = c.login.Save(ctx)
		if err != nil {
			c.meta.ConfirmationsSynced, c.meta.ConfirmationVersion = oldSynced, oldVersion
			c.meta.Rescan = true
		}
		c.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return pending
}
func (c *Client) saveMediaRetry(ctx context.Context, state *RoomState, id, after, revision string, failed bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	old, exists := state.Media[id]
	if !failed && !exists {
		return nil
	}
	if state.Media == nil {
		state.Media = make(map[string]MediaRetry)
	}
	if failed {
		delay := min(max(old.Delay*2, 30*time.Second), 15*time.Minute)
		next := time.Now().Add(delay)
		if exists && after != old.After && after != old.Start {
			delay = max(old.Delay, 30*time.Second)
			next = time.Now().Add(30 * time.Second)
		}
		start := old.Start
		if !exists {
			start = after
		}
		state.Media[id] = MediaRetry{Start: start, After: after, Revision: revision, Next: next, Delay: delay}
	} else {
		delete(state.Media, id)
	}
	if err := c.login.Save(ctx); err != nil {
		if exists {
			state.Media[id] = old
		} else {
			delete(state.Media, id)
		}
		return err
	}
	return nil
}
func (c *Client) deliverMedia(ctx context.Context, state *RoomState, m chatgpt.Message, after string) error {
	rev := revision(m)
	if retry, exists := state.Media[m.ID]; exists && retry.Revision == rev && time.Now().Before(retry.Next) {
		return mediaRetryError{errors.New("attachment retry scheduled")}
	}
	if retry, exists := state.Media[m.ID]; exists {
		after = retry.After
	}
	err := c.deliver(ctx, state, m)
	if err == nil {
		err = c.syncReactions(ctx, state, m)
	}
	if err == nil || onlyMediaRetries(err) {
		if saveErr := c.saveMediaRetry(ctx, state, m.ID, after, rev, err != nil); saveErr != nil {
			return saveErr
		}
	}
	return err
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
	existing, err := c.connector.bridge.DB.Message.GetAllPartsByID(ctx, c.login.ID, networkid.MessageID(m.ID))
	if err != nil {
		return err
	}
	if incomingStale(m, existing) {
		return nil
	}
	c.prepareThreads(state, &m, existing)
	var cleanup func() error
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
	var conversionErr error
	msg.ConvertMessageFunc = func(ctx context.Context, p *bridgev2.Portal, intent bridgev2.MatrixAPI, _ chatgpt.Message) (*bridgev2.ConvertedMessage, error) {
		converted, err := c.convertIncoming(ctx, state, p, intent, m, nil)
		conversionErr = err
		if err == nil {
			for _, part := range converted.Parts {
				cutOffLongText(c.login.Log, part.Content, maxMatrixContentBytes)
			}
		}
		return converted, err
	}
	msg.ConvertEditFunc = func(ctx context.Context, p *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, _ chatgpt.Message) (*bridgev2.ConvertedEdit, error) {
		converted, err := c.convertIncoming(ctx, state, p, intent, m, existing)
		conversionErr = err
		if err != nil {
			return nil, err
		}
		addedParts := *converted
		addedParts.Parts = nil
		result := &bridgev2.ConvertedEdit{AddedParts: &addedParts}
		byID := make(map[networkid.PartID]*database.Message, len(existing))
		for _, part := range existing {
			byID[part.PartID] = part
		}
		for _, part := range converted.Parts {
			if old := byID[part.ID]; old != nil {
				cutOffLongText(c.login.Log, part.Content, maxMatrixContentBytes/2)
				edit := part.ToEditPart(old)
				if md, ok := part.DBMetadata.(*MessageMetadata); ok && md.Thread != nil {
					edit.TopLevelExtra = map[string]any{"com.beeper.dont_render_edited": true}
				}
				result.ModifiedParts = append(result.ModifiedParts, edit)
			} else {
				cutOffLongText(c.login.Log, part.Content, maxMatrixContentBytes)
				result.AddedParts.Parts = append(result.AddedParts.Parts, part)
			}
		}
		return result, nil
	}
	msg.HandleExistingFunc = func(ctx context.Context, p *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, _ chatgpt.Message) (bridgev2.UpsertResult, error) {
		cleanup = func() error {
			expected := expectedIncomingParts(m, existing)
			for _, old := range existing {
				if !expected[old.PartID] {
					_, err := intent.SendMessage(ctx, p.MXID, event.EventRedaction, &event.Content{Parsed: &event.RedactionEventContent{Redacts: old.MXID}}, &bridgev2.MatrixSendExtra{Timestamp: m.Updated})
					if err != nil {
						return errors.New("Matrix attachment redaction failed; checkpoint retained")
					}
					if err = c.connector.bridge.DB.Message.Delete(ctx, old.RowID); err != nil {
						return err
					}
				}
			}
			return nil
		}
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
	conversionFailure := func() error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var storage *chatgpt.AttachmentStorageError
		if errors.As(conversionErr, &storage) {
			return mediaRetryError{storage}
		}
		return conversionErr
	}
	result := c.login.QueueRemoteEvent(msg)
	if !result.Success || result.Ignored {
		if err := conversionFailure(); err != nil {
			return err
		}
		return errors.New("bridge could not queue remote message; checkpoint retained")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	if err := conversionFailure(); err != nil {
		return err
	}
	saved, err := c.connector.bridge.DB.Message.GetAllPartsByID(ctx, c.login.ID, networkid.MessageID(m.ID))
	if err != nil {
		return err
	}
	if !incomingSaved(m, saved) {
		return errors.New("remote message parts not durably saved; checkpoint retained")
	}
	if cleanup != nil {
		if err = cleanup(); err != nil {
			return err
		}
	}
	if err = c.saveMessageMetadata(ctx, m, saved); err != nil {
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
		return nil, bridgev2.ErrUnsupportedMessageType
	}
	replyTo, err := c.matrixReplyTarget(msg)
	if err != nil {
		return nil, bridgev2.WrapErrorInStatus(err).
			WithMessage("this reply target isn't available in this ChatGPT chat").
			WithErrorReason(event.MessageStatusUnsupported).
			WithStatus(event.MessageStatusFail).
			WithIsCertain(true).
			WithSendNotice(true)
	}
	requestID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(string(c.login.ID)+"\x00"+string(msg.Event.ID))).String()
	c.mu.RLock()
	api := c.api
	runContext := c.runContext
	state := c.meta.Rooms[string(msg.Portal.ID)]
	prior, hadPrior := c.meta.Pending[requestID]
	c.mu.RUnlock()
	certain := !hadPrior || prior.KnownUnsent
	if api == nil || runContext == nil || runContext.Err() != nil || !c.connected.Load() {
		return nil, bridgev2.WrapErrorInStatus(errors.New("not connected to ChatGPT")).
			WithMessage("not connected to ChatGPT, try again in a moment").
			WithIsCertain(certain).
			WithSendNotice(true)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(runContext, cancel)
	defer stop()
	if state == nil || msg.Portal.Receiver != c.login.ID {
		return nil, relinkStatus(errors.New("refusing unverified Dot room"), certain)
	}
	room, err := api.Verify(ctx, state.Profile)
	if err != nil {
		c.state(err)
		return nil, wrapSendError(err, certain)
	}
	current, err := roomState(api, state.Profile, room)
	if err != nil {
		return nil, relinkStatus(err, certain)
	}
	if current.DotMember != state.DotMember || current.HumanMember != state.HumanMember {
		return nil, relinkStatus(errors.New("dot room members changed; refusing send"), certain)
	}
	fingerprint := []byte(msg.Content.Body)
	text := msg.Content.Body
	if media {
		text = msg.Content.GetCaption()
		fingerprint, err = json.Marshal(msg.Content)
		if err != nil {
			return nil, errors.New("cannot fingerprint attachment content")
		}
		if msg.Content.Info != nil && msg.Content.Info.Size > chatgpt.MaxUploadSize {
			return nil, errMediaTooLarge
		}
	}
	if replyTo != "" {
		fingerprint = append(append(fingerprint, '\x00'), []byte(replyTo)...)
	}
	hash := sha256.Sum256(fingerprint)
	bodyHash := hex.EncodeToString(hash[:])
	c.mu.Lock()
	if c.meta.Pending == nil {
		c.meta.Pending = map[string]PendingSend{}
	}
	pending, retry := c.meta.Pending[requestID]
	wasUncertain := retry && !pending.KnownUnsent
	if retry && (pending.BodyHash != bodyHash || pending.Room != state.Profile.Room || pending.EventID != msg.Event.ID || pending.Media != media || pending.ReplyTo != replyTo) {
		c.mu.Unlock()
		return nil, bridgev2.WrapErrorInStatus(errors.New("retry content differs from the original request; refusing to reuse its request ID")).
			WithMessage("this retry doesn't match the original message, so it can't be sent").
			WithStatus(event.MessageStatusFail).
			WithIsCertain(!wasUncertain).
			WithSendNotice(true)
	}
	if !retry {
		pending = PendingSend{Room: state.Profile.Room, EventID: msg.Event.ID, Sender: msg.Event.Sender, Timestamp: msg.Event.Timestamp, BodyHash: bodyHash, Media: media, KnownUnsent: true, ReplyTo: replyTo}
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
			return nil, wrapSendError(err, false)
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
				return nil, wrapSendError(fmt.Errorf("attachment upload failed; no message sent (an uncertain upload may leave an unused file): %w", err), true)
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
	m, err := api.Send(ctx, state.Profile.Room, text, requestID, attachments, replyTo)
	if err != nil {
		certain := !wasUncertain && isRejection(err)
		var httpErr *chatgpt.HTTPError
		if errors.As(err, &httpErr) && (httpErr.Status == 401 || httpErr.Status == 403) {
			c.state(err)
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
		return nil, wrapSendError(err, certain)
	}
	if m.Sender != state.HumanMember {
		return nil, errors.New("send response sender identity mismatch; delivery unknown")
	}
	return c.sendResponse(*m, media, runContext), nil
}

func (c *Client) sendResponse(m chatgpt.Message, media bool, generation context.Context) *bridgev2.MatrixMessageResponse {
	return &bridgev2.MatrixMessageResponse{DB: &database.Message{ID: networkid.MessageID(m.ID), SenderID: c.GetUserID(), ThreadRoot: networkid.MessageID(m.ReplyRootMessageID), Timestamp: m.Created, Metadata: &MessageMetadata{Revision: revision(m), OutgoingMedia: media}}, PostSave: func(ctx context.Context, _ *database.Message) {
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
	c.mu.RLock()
	replyTo := c.meta.Pending[requestID].ReplyTo
	c.mu.RUnlock()
	if replyTo != "" {
		return c.findReplyRequest(ctx, api, state, requestID, replyTo)
	}
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
