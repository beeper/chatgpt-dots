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
	avatarRefreshInterval = 30 * time.Minute
	avatarRetryInterval   = 30 * time.Minute
	dotRecheckInterval    = 5 * time.Minute
	uncertainSendWindow   = 2 * time.Minute
	maxMessageAttempts    = 3
)

type Client struct {
	connector      *Connector
	login          *bridgev2.UserLogin
	meta           *LoginMetadata
	lifecycle      sync.Mutex
	sendMu         sync.Mutex
	stateMu        sync.Mutex
	mu             sync.RWMutex
	api            *chatgpt.Client
	runContext     context.Context
	cancel         context.CancelFunc
	done           chan struct{}
	loggedOut      atomic.Bool
	proxy          atomic.Pointer[proxyTransport]
	threadMessages map[string]*threadMessage
	signalWake     chan struct{}
	rescan         atomic.Bool
	recheck        atomic.Bool
	refreshMu      sync.Mutex
	refresh        map[string]map[string]bool
	receipts       map[string]*receiptState
	delivered      map[string][32]byte
	deliveredOrder []string
	selfAvatarFail string
	profileStale   atomic.Bool
	selfAvatarTime time.Time
	failures       map[string]int
	rescanAfter    map[string]string
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
	c.api = nil
	c.mu.Unlock()
}
func (c *Client) IsLoggedIn() bool { return !c.loggedOut.Load() }
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

func (c *Client) sendState(state status.BridgeState) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	switch {
	case state.StateEvent == status.StateBadCredentials, state.Error == stateDotMissing:
		c.loggedOut.Store(true)
	case state.StateEvent == status.StateConnected:
		c.loggedOut.Store(false)
	}
	c.login.BridgeState.Send(state)
}
func (c *Client) state(err error) bool {
	return c.stateVia(err, c.proxy.Load().host())
}
func (c *Client) stateVia(err error, proxyHost string) bool {
	state := status.BridgeState{StateEvent: status.StateTransientDisconnect, Error: stateSyncFailed, Info: map[string]any{"go_error": err.Error()}}
	var httpErr *chatgpt.HTTPError
	var schemaErr *chatgpt.SchemaError
	if errors.Is(err, errDotUnavailable) {
		state = badCredentialsState(stateDotUnavailable, err)
	} else if errors.Is(err, errDotMissing) {
		state.Error = stateDotMissing
	} else if errors.As(err, &httpErr) {
		c.login.Log.Warn().Err(err).
			Int("status", httpErr.Status).
			Str("path", httpErr.Path).
			Bool("cloudflare_challenge", httpErr.Challenge).
			Str("detail", httpErr.Body).
			Str("proxy_host", proxyHost).
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
	} else if errors.As(err, &schemaErr) {
		c.login.Log.Warn().Err(err).
			Str("path", schemaErr.Path).
			Int("status", schemaErr.Status).
			Int64("content_length", schemaErr.ContentLength).
			Int64("bytes_read", schemaErr.BytesRead).
			Bool("truncated", schemaErr.Truncated).
			Str("decode_error", schemaErr.Cause).
			Str("decode_field", schemaErr.Field).
			Int64("decode_offset", schemaErr.Offset).
			Str("proxy_host", proxyHost).
			Msg("ChatGPT request failed")
	} else if isTransportError(err) {
		state.Error = stateNetworkError
	}
	c.sendState(state)
	return state.StateEvent == status.StateBadCredentials
}
func (c *Client) run(ctx context.Context) {
	defer close(c.done)
	c.signalWake = make(chan struct{}, 1)
	c.receipts = make(map[string]*receiptState)
	c.delivered = make(map[string][32]byte)
	c.deliveredOrder = nil
	c.recheck.Store(true)
	c.failures = make(map[string]int)
	c.rescanAfter = make(map[string]string)
	for _, watch := range c.threadMessages {
		watch.NextRefresh = time.Time{}
	}
	c.login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnecting})
	creds, err := c.connector.readCredentials(ctx, c.login)
	if errors.Is(err, errNoCredentials) {
		c.sendState(badCredentialsState(stateNoCredentials, err))
		return
	} else if err != nil {
		c.sendState(badCredentialsState(stateBadCredentials, err))
		return
	}
	proxy := &proxyTransport{connector: c.connector, log: c.login.Log}
	c.proxy.Store(proxy)
	api, err := chatgpt.New(creds, proxy)
	if err != nil {
		c.sendState(badCredentialsState(stateBadCredentials, err))
		return
	}
	if api.Identity.Account != c.meta.Account || api.Identity.User != c.meta.User {
		c.sendState(badCredentialsState(stateIdentityChanged, errors.New("credential identity changed")))
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
	recheck := false
	for c.connectDot(ctx, api, proxy, recheck) && c.serve(ctx, api, proxy) && wait(ctx, dotRecheckInterval) {
		recheck = true
	}
}
func (c *Client) connectDot(ctx context.Context, api *chatgpt.Client, proxy *proxyTransport, recheck bool) bool {
	delay := time.Second * 3
	recheckDelay := dotRecheckInterval
	if recheck {
		recheckDelay *= 2
	}
	for ctx.Err() == nil {
		err := proxy.update(ctx, "connect")
		if err == nil {
			err = c.verify(ctx, api)
		}
		if err == nil {
			return true
		}
		next := delay
		if errors.Is(err, errDotMissing) && recheck {
			err = fmt.Errorf("%w: %w", errDotUnavailable, err)
		}
		if errors.Is(err, errDotReplaced) {
			c.state(err)
			return false
		} else if errors.Is(err, errDotUnavailable) || errors.Is(err, errDotMissing) {
			c.state(err)
			recheck = true
			next = recheckDelay
			recheckDelay = min(recheckDelay*2, time.Hour)
		} else if c.state(err) {
			return false
		}
		if !wait(ctx, next) {
			return false
		}
		delay = min(delay*2, time.Minute)
	}
	return false
}
func (c *Client) serve(ctx context.Context, api *chatgpt.Client, proxy *proxyTransport) bool {
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
	delay := pollInterval
	nextAvatarRefresh := time.Now().Add(avatarRefreshInterval)
	pollNow := true
	for ctx.Err() == nil {
		var err error
		if pollNow || c.mediaRetryDue() || !time.Now().Before(c.uncertainSendDeadline()) {
			pollNow = false
			err = c.poll(ctx, api)
		}
		err = errors.Join(err, c.refreshThreads(ctx, api))
		if !time.Now().Before(nextAvatarRefresh) {
			nextAvatarRefresh = time.Now().Add(avatarRefreshInterval)
			if profileErr := c.verify(ctx, api); errors.Is(profileErr, errDotUnavailable) || errors.Is(profileErr, errDotMissing) {
				err = errors.Join(err, profileErr)
			} else if profileErr != nil {
				c.login.Log.Warn().Err(profileErr).Msg("Could not refresh ChatGPT profiles")
			}
		}
		if errors.Is(err, errDotUnavailable) || errors.Is(err, errDotMissing) {
			c.state(err)
			return !errors.Is(err, errDotReplaced)
		}
		if ctx.Err() != nil {
			return false
		}
		proxyHost := proxy.host()
		if proxy.failed.Load() {
			if proxyErr := proxy.update(ctx, "connect"); proxyErr != nil {
				c.login.Log.Warn().Err(proxyErr).Msg("Could not refresh ChatGPT proxy")
				err = errors.Join(err, proxyErr)
			}
		}
		wake := c.nextWake(nextAvatarRefresh)
		if err != nil && !onlyMediaRetries(err) {
			if ctx.Err() != nil {
				return false
			}
			if c.stateVia(err, proxyHost) {
				return false
			}
			delay = min(delay*2, time.Minute)
			pollNow = true
			wake = time.Now().Add(delay)
		} else {
			if err != nil {
				c.login.Log.Debug().Err(err).Msg("Waiting to retry a ChatGPT attachment download")
			}
			if c.login.BridgeState.GetPrevUnsent().StateEvent != status.StateConnected {
				c.sendState(status.BridgeState{StateEvent: status.StateConnected})
			}
			delay = pollInterval
		}
		timer := time.NewTimer(time.Until(wake))
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-c.signalWake:
			timer.Stop()
			pollNow = true
		case <-timer.C:
		}
	}
	return false
}
func (c *Client) mediaRetryDue() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, state := range c.meta.Rooms {
		for _, retry := range state.Media {
			if !time.Now().Before(retry.Next) {
				return true
			}
		}
	}
	return false
}
func (c *Client) uncertainSendDeadline() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var deadline time.Time
	for _, pending := range c.meta.Pending {
		if pending.KnownUnsent {
			continue
		}
		attempted := pending.Attempted
		if attempted.IsZero() {
			attempted = time.UnixMilli(pending.Timestamp)
		}
		if due := attempted.Add(uncertainSendWindow); deadline.IsZero() || due.Before(deadline) {
			deadline = due
		}
	}
	if deadline.IsZero() {
		return time.Now().Add(avatarRefreshInterval)
	}
	return deadline
}
func (c *Client) nextWake(next time.Time) time.Time {
	if deadline := c.uncertainSendDeadline(); deadline.Before(next) {
		next = deadline
	}
	for _, watch := range c.threadMessages {
		if _, pending := watch.Room.Media[watch.Message.ID]; !pending && watch.NextRefresh.Before(next) {
			next = watch.NextRefresh
		}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, state := range c.meta.Rooms {
		for _, retry := range state.Media {
			if retry.Next.Before(next) {
				next = retry.Next
			}
		}
	}
	if earliest := time.Now().Add(pollInterval); next.Before(earliest) {
		next = earliest
	}
	return next
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
			return c.checkDotReplaced(ctx, api, state, err)
		}
		current, err := roomMembers(api, state.Profile, r)
		if err != nil {
			return err
		}
		if current.DotMember != state.DotMember || current.HumanMember != state.HumanMember {
			return errors.New("Dot room members changed; refusing to change existing identity mappings")
		}
		changed := c.refreshSelfAvatar(ctx, api, r.Member(state.HumanMember))
		c.refreshDotAvatar(api, state, r.Member(state.DotMember))
		c.mu.Lock()
		if name := r.Member(state.DotMember).Name; name != "" && name != state.Profile.Name {
			state.Profile.Name = name
			changed = true
		}
		changed = changed || state.HumanName != current.HumanName || c.login.RemoteProfile.Name != current.HumanName
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
		for _, userID := range []networkid.UserID{c.GetUserID(), c.dotID(state)} {
			ghost, err := c.connector.bridge.GetGhostByID(ctx, userID)
			if err != nil {
				return err
			}
			info, err := c.GetUserInfo(ctx, ghost)
			if err != nil {
				return err
			}
			ghost.UpdateInfo(ctx, info)
		}
		portal, err := c.connector.bridge.GetPortalByKey(ctx, c.key(state))
		if err != nil {
			return err
		}
		info := c.chatInfo(state)
		if portal.MXID == "" {
			if err = portal.CreateMatrixRoom(ctx, c.login, info); err != nil {
				return err
			}
			clear(c.delivered)
			c.deliveredOrder = nil
			c.requestSync(true)
		} else {
			info.Members = nil
			portal.UpdateInfo(ctx, info, nil, nil, time.Time{})
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
	if replaced {
		return fmt.Errorf("%w: %w: %w", errDotUnavailable, errDotReplaced, err)
	} else if missing {
		return fmt.Errorf("%w: %w", errDotMissing, err)
	}
	return err
}
func (c *Client) refreshSelfAvatar(ctx context.Context, api *chatgpt.Client, member chatgpt.RoomMember) bool {
	avatarID := member.AvatarID()
	c.mu.RLock()
	unchanged := avatarID == c.meta.SelfAvatarID && (c.login.RemoteProfile.Avatar != "") == (avatarID != "")
	c.mu.RUnlock()
	recent := time.Since(c.selfAvatarTime) < avatarRetryInterval
	if unchanged && (!member.AvatarContentMayChange() || recent) || avatarID == c.selfAvatarFail && recent {
		return false
	}
	c.selfAvatarTime = time.Now()
	var mxc id.ContentURIString
	var file *event.EncryptedFileInfo
	hash := ""
	if avatarID != "" {
		data, err := api.SelfAvatar(ctx, member.AvatarURL)
		if err == nil {
			sum := sha256.Sum256(data)
			hash = hex.EncodeToString(sum[:])
			if unchanged && hash == c.meta.SelfAvatarHash {
				return false
			}
			mxc, file, err = c.connector.bridge.Bot.UploadMedia(ctx, "", data, "avatar", http.DetectContentType(data))
		}
		if err != nil {
			c.login.Log.Warn().Err(err).Msg("Could not update own account avatar")
			c.selfAvatarFail = avatarID
			return false
		}
	}
	c.mu.Lock()
	c.login.RemoteProfile.Avatar, c.login.RemoteProfile.AvatarFile = mxc, file
	c.meta.SelfAvatarID, c.meta.SelfAvatarHash = avatarID, hash
	c.mu.Unlock()
	return true
}
func (c *Client) profileChanged(state *RoomState, room *chatgpt.Room) bool {
	dot, self := room.Member(state.DotMember), room.Member(state.HumanMember).AvatarID()
	c.mu.RLock()
	defer c.mu.RUnlock()
	return dot.Name != "" && dot.Name != state.Profile.Name || state.Avatar != nil && string(state.Avatar.ID) != dot.AvatarID() || self != c.meta.SelfAvatarID && self != c.selfAvatarFail
}
func (c *Client) refreshDotAvatar(api *chatgpt.Client, state *RoomState, member chatgpt.RoomMember) {
	avatarID := networkid.AvatarID(member.AvatarID())
	c.mu.Lock()
	defer c.mu.Unlock()
	if avatarID == "" {
		state.Avatar = &bridgev2.Avatar{Remove: true}
		return
	}
	if state.Avatar != nil && state.Avatar.ID == avatarID && (state.avatarFailed.IsZero() || time.Since(state.avatarFailed) < avatarRetryInterval) {
		return
	}
	state.avatarFailed = time.Time{}
	var once sync.Once
	var data []byte
	var err error
	state.Avatar = &bridgev2.Avatar{ID: avatarID, Get: func(ctx context.Context) ([]byte, error) {
		once.Do(func() {
			if data, err = api.DotAvatar(ctx, member.AvatarURL); err != nil {
				c.login.Log.Warn().Err(err).Msg("Could not update Dot avatar")
				c.mu.Lock()
				state.avatarFailed = time.Now()
				c.mu.Unlock()
			}
		})
		return data, err
	}}
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
func (c *Client) HandleMatrixMembership(_ context.Context, msg *bridgev2.MatrixMembershipChange) (*bridgev2.MatrixMembershipResult, error) {
	if msg.Type == bridgev2.Invite {
		return nil, errors.New("cannot invite additional users to a Dot chat")
	}
	return nil, nil
}
func (c *Client) GetCapabilities(context.Context, *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{ID: "chatgpt-dots.incoming-typing.v1", ReadReceipts: true, ReactionCount: 1, Formatting: event.FormattingFeatureMap{
		event.FmtBold:          event.CapLevelFullySupported,
		event.FmtItalic:        event.CapLevelFullySupported,
		event.FmtStrikethrough: event.CapLevelFullySupported,
		event.FmtInlineCode:    event.CapLevelFullySupported,
		event.FmtCodeBlock:     event.CapLevelFullySupported,
		event.FmtBlockquote:    event.CapLevelFullySupported,
		event.FmtInlineLink:    event.CapLevelFullySupported,
		event.FmtUnorderedList: event.CapLevelFullySupported,
		event.FmtOrderedList:   event.CapLevelFullySupported,
		event.FmtListStart:     event.CapLevelFullySupported,
		event.FmtHeaders:       event.CapLevelPartialSupport,
	}, File: event.FileFeatureMap{
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
	recheck := false
	full := c.rescan.Swap(false)
	if full {
		clear(c.rescanAfter)
	}
	full = full || c.meta.Rescan || !c.meta.ConfirmationsSynced || c.meta.ConfirmationVersion != confirmationRenderingVersion
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
	}
	if c.recheck.Swap(false) {
		recheck = true
		defer func() {
			if err != nil && !onlyMediaRetries(err) {
				c.recheck.Store(true)
			}
		}()
	}
	var pending error
	for _, state := range c.meta.Rooms {
		receipt, err := c.readReceipt(ctx, api, state)
		if err != nil {
			return err
		}
		if c.profileStale.Swap(false) {
			if profileErr := c.verify(ctx, api); profileErr != nil {
				c.login.Log.Warn().Err(profileErr).Msg("Could not refresh ChatGPT profiles")
			}
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
		after := state.Cursor
		if recheck && len(state.Overlap) > 0 {
			after = state.Overlap[0]
		}
		if full {
			after = state.Baseline
			if resume := c.rescanAfter[state.Profile.Room]; resume != "" {
				after = resume
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
		searchFrom := after
		if !full && len(state.Overlap) > 0 {
			searchFrom = state.Overlap[0]
		}
		for _, requestID := range pendingIDs {
			m, err := c.findRequest(ctx, api, state, requestID, searchFrom)
			if err != nil {
				return err
			}
			if m != nil {
				if err = c.deliverMedia(ctx, state, *m, searchFrom); err != nil {
					if !onlyMediaRetries(err) {
						return err
					}
					pending = errors.Join(pending, err)
				}
			} else if err = c.expireUncertainSend(ctx, state, requestID); err != nil {
				return err
			}
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
					c.login.Log.Error().Str("message_id", m.ID).Msg("Skipping ChatGPT message without a stable identity or timestamp")
					continue
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
			if full {
				c.rescanAfter[state.Profile.Room] = next
			}
			after = next
		}
		if err := c.refreshMessages(ctx, api, state); err != nil {
			if !onlyMediaRetries(err) {
				return err
			}
			pending = errors.Join(pending, err)
		}
		if err := c.sendReceipt(ctx, state, receipt); err != nil {
			return err
		}
		if err := c.sendHumanReceipt(ctx, state, receipt); err != nil {
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
		clear(c.rescanAfter)
	}
	return pending
}
func (c *Client) refreshMessages(ctx context.Context, api *chatgpt.Client, state *RoomState) error {
	c.refreshMu.Lock()
	queued := c.refresh[state.Profile.Room]
	delete(c.refresh, state.Profile.Room)
	c.refreshMu.Unlock()
	var pending error
	for id, changed := range queued {
		err := c.refreshMessage(ctx, api, state, id, changed)
		if err != nil && !onlyMediaRetries(err) {
			c.refreshMu.Lock()
			for id, changed := range queued {
				if c.refresh == nil {
					c.refresh = make(map[string]map[string]bool)
				}
				if c.refresh[state.Profile.Room] == nil {
					c.refresh[state.Profile.Room] = make(map[string]bool)
				}
				c.refresh[state.Profile.Room][id] = c.refresh[state.Profile.Room][id] || changed
			}
			c.refreshMu.Unlock()
			return err
		}
		pending = errors.Join(pending, err)
		delete(queued, id)
	}
	return pending
}
func (c *Client) refreshMessage(ctx context.Context, api *chatgpt.Client, state *RoomState, id string, changed bool) error {
	existing, err := c.connector.bridge.DB.Message.GetFirstPartByID(ctx, c.login.ID, networkid.MessageID(id))
	if err != nil {
		return err
	}
	if existing != nil && !changed {
		return nil
	}
	m, previous, err := api.MessageAndPrevious(ctx, state.Profile.Room, id)
	if errors.Is(err, chatgpt.ErrMessageNotFound) {
		c.login.Log.Debug().Str("message_id", id).Msg("ChatGPT no longer returns a message it reported as changed")
		return nil
	} else if err != nil {
		return err
	}
	if previous == "" {
		previous = state.Baseline
	}
	return c.deliverMedia(ctx, state, *m, previous)
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
	if err != nil && !onlyMediaRetries(err) && c.giveUp(ctx, m, err) {
		err = nil
	}
	if err == nil {
		delete(c.failures, m.ID)
	}
	if err == nil || onlyMediaRetries(err) {
		if saveErr := c.saveMediaRetry(ctx, state, m.ID, after, rev, err != nil); saveErr != nil {
			return saveErr
		}
	}
	return err
}
func (c *Client) giveUp(ctx context.Context, m chatgpt.Message, err error) bool {
	var httpErr *chatgpt.HTTPError
	if !errors.As(err, &messageError{}) || ctx.Err() != nil || isTransportError(err) || errors.As(err, &httpErr) && !isMessageRejection(httpErr) {
		return false
	}
	c.failures[m.ID]++
	if c.failures[m.ID] < maxMessageAttempts {
		return false
	}
	c.login.Log.Err(err).Str("message_id", m.ID).Int("attempts", c.failures[m.ID]).Msg("Skipping ChatGPT message that keeps failing to bridge")
	delete(c.failures, m.ID)
	return true
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
		return messageError{errors.New("message from an unverified Dot room member")}
	}
	existing, err := c.connector.bridge.DB.Message.GetAllPartsByID(ctx, c.login.ID, networkid.MessageID(m.ID))
	if err != nil {
		return err
	}
	if incomingStale(m, existing) {
		return c.clearPending(ctx, m.RequestID)
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
		original.AddPendingToSave(&database.Message{SenderID: c.GetUserID(), Metadata: &MessageMetadata{Revision: textRevision(m, revision(m)), OutgoingMedia: pending.Media}}, networkid.TransactionID(m.RequestID), nil)
	}
	var conversionErr error
	msg.ConvertMessageFunc = func(ctx context.Context, p *bridgev2.Portal, intent bridgev2.MatrixAPI, _ chatgpt.Message) (*bridgev2.ConvertedMessage, error) {
		converted, err := c.convertIncoming(ctx, state, p, intent, m, nil)
		conversionErr = err
		if err != nil && c.failures[m.ID]+1 < maxMessageAttempts {
			return nil, fmt.Errorf("%w: %w", bridgev2.ErrIgnoringRemoteEvent, err)
		}
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
		if err != nil && c.failures[m.ID]+1 < maxMessageAttempts {
			return nil, fmt.Errorf("%w: %w", bridgev2.ErrIgnoringRemoteEvent, err)
		} else if err != nil {
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
		} else if conversionErr != nil {
			return messageError{conversionErr}
		}
		return nil
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
	return c.clearPending(ctx, m.RequestID)
}
func (c *Client) clearPending(ctx context.Context, requestID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending, exists := c.meta.Pending[requestID]
	if !exists {
		return nil
	}
	delete(c.meta.Pending, requestID)
	if err := c.login.Save(ctx); err != nil {
		c.meta.Pending[requestID] = pending
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
	var searchFrom string
	if state != nil {
		searchFrom = state.Cursor
		if len(state.Overlap) > 0 {
			searchFrom = state.Overlap[0]
		}
	}
	c.mu.RUnlock()
	certain := !hadPrior || prior.KnownUnsent
	if api == nil || runContext == nil || runContext.Err() != nil || !c.IsLoggedIn() {
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
		c.requestSync(false)
		return nil, wrapSendError(err, certain)
	}
	current, err := roomMembers(api, state.Profile, room)
	if err != nil {
		return nil, relinkStatus(err, certain)
	}
	if current.DotMember != state.DotMember || current.HumanMember != state.HumanMember {
		return nil, relinkStatus(errors.New("dot room members changed; refusing send"), certain)
	}
	fingerprint := []byte(msg.Content.Body)
	text := matrixText(ctx, msg.Content, media)
	if media {
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
	} else if !pending.KnownUnsent {
		pending.Attempted = time.Now()
	}
	c.meta.Pending[requestID] = pending
	err = c.login.Save(ctx)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if retry && !pending.KnownUnsent {
		found, err := c.findRequest(ctx, api, state, requestID, searchFrom)
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
			fileName := msg.Content.GetFileName()
			if isHEICImage(mimeType, fileName) && looksLikeHEIF(data) {
				data, err = convertHEICToJPEG(ctx, data)
				if err != nil {
					return nil, fmt.Errorf("%w: %w", bridgev2.ErrMediaConvertFailed, err)
				}
				if len(data) > chatgpt.MaxUploadSize {
					return nil, errMediaTooLarge
				}
				mimeType = "image/jpeg"
				fileName = jpegFileName(fileName)
			}
			pending.FileID, err = api.Upload(ctx, state.Profile.Room, fileName, mimeType, data)
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
	pending.Attempted = time.Now()
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
			c.requestSync(false)
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
	return &bridgev2.MatrixMessageResponse{DB: &database.Message{ID: networkid.MessageID(m.ID), SenderID: c.GetUserID(), ThreadRoot: networkid.MessageID(m.ReplyRootMessageID), Timestamp: m.Created, Metadata: &MessageMetadata{Revision: textRevision(m, revision(m)), OutgoingMedia: media}}, PostSave: func(ctx context.Context, _ *database.Message) {
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
func (c *Client) expireUncertainSend(ctx context.Context, state *RoomState, requestID string) error {
	c.mu.Lock()
	pending, ok := c.meta.Pending[requestID]
	attempted := pending.Attempted
	if attempted.IsZero() {
		attempted = time.UnixMilli(pending.Timestamp)
	}
	if !ok || pending.KnownUnsent || time.Since(attempted) < uncertainSendWindow {
		c.mu.Unlock()
		return nil
	}
	expired := pending
	expired.KnownUnsent = true
	c.meta.Pending[requestID] = expired
	if err := c.login.Save(ctx); err != nil {
		c.meta.Pending[requestID] = pending
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	c.login.Log.Warn().Str("request_id", requestID).Stringer("event_id", pending.EventID).Time("attempted", attempted).Msg("ChatGPT never listed an uncertain send; marking it failed")
	portal, err := c.connector.bridge.GetExistingPortalByKey(ctx, c.key(state))
	if err != nil || portal == nil || portal.MXID == "" {
		return err
	}
	ms := bridgev2.WrapErrorInStatus(errors.New("uncertain send never appeared in the Dot chat")).
		WithStatus(event.MessageStatusRetriable).
		WithErrorReason(event.MessageStatusNetworkError).
		WithMessage("ChatGPT didn't receive this message, try sending it again").
		WithIsCertain(true).
		WithSendNotice(true)
	c.connector.bridge.Matrix.SendMessageStatus(ctx, &ms, &bridgev2.MessageStatusEventInfo{RoomID: portal.MXID, SourceEventID: pending.EventID, EventType: event.EventMessage, Sender: pending.Sender})
	return nil
}
func (c *Client) findRequest(ctx context.Context, api *chatgpt.Client, state *RoomState, requestID, after string) (*chatgpt.Message, error) {
	c.mu.RLock()
	replyTo := c.meta.Pending[requestID].ReplyTo
	c.mu.RUnlock()
	if replyTo != "" {
		return c.findReplyRequest(ctx, api, state, requestID, replyTo)
	}
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
var _ bridgev2.MembershipHandlingNetworkAPI = (*Client)(nil)
