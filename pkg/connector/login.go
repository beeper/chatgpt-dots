package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/id"
)

type LoginMetadata struct {
	CredentialRef  string                 `json:"credential_ref,omitempty"`
	Credentials    *chatgpt.Credentials   `json:"credentials,omitempty"`
	Account        string                 `json:"account"`
	User           string                 `json:"user"`
	Rooms          map[string]*RoomState  `json:"rooms"`
	Pending        map[string]PendingSend `json:"pending,omitempty"`
	SelfAvatarHash string                 `json:"self_avatar_hash,omitempty"`
}
type PendingSend struct {
	// Missing in older records means delivery may have happened.
	KnownUnsent bool       `json:"known_unsent,omitempty"`
	FileID      string     `json:"file_id,omitempty"`
	Media       bool       `json:"media,omitempty"`
	Room        string     `json:"room"`
	EventID     id.EventID `json:"event_id"`
	Sender      id.UserID  `json:"sender"`
	Timestamp   int64      `json:"timestamp"`
	BodyHash    string     `json:"body_hash"`
}
type RoomState struct {
	Avatar      *bridgev2.Avatar `json:"-"`
	Profile     chatgpt.Profile  `json:"profile"`
	DotMember   string           `json:"dot_member"`
	HumanMember string           `json:"human_member"`
	HumanName   string           `json:"human_name,omitempty"`
	Linked      time.Time        `json:"linked"`
	Baseline    string           `json:"baseline"`
	Cursor      string           `json:"cursor"`
	Overlap     []string         `json:"overlap"`
}
type MessageMetadata struct {
	OutgoingMedia bool                  `json:"outgoing_media,omitempty"`
	TextAnchor    bool                  `json:"text_anchor,omitempty"`
	Revision      string                `json:"revision"`
	Thread        *chatgpt.ThreadStatus `json:"thread,omitempty"`
}
type Login struct {
	connector *Connector
	user      *bridgev2.User
	mu        sync.Mutex
	cancel    context.CancelFunc
	cancelled bool
}

func (c *Connector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{{ID: "cookies", Name: "ChatGPT Dots", Description: "Sign in with ChatGPT to message your existing Dot. Regular ChatGPT conversations are not imported."}}
}
func (c *Connector) CreateLogin(_ context.Context, u *bridgev2.User, flow string) (bridgev2.LoginProcess, error) {
	if flow != "cookies" {
		return nil, errors.New("unknown login flow")
	}
	return &Login{connector: c, user: u}, nil
}
func (l *Login) Start(context.Context) (*bridgev2.LoginStep, error) {
	instructions := "Connect ChatGPT Dots: sign in with ChatGPT to message your existing Dot. Regular ChatGPT conversations are not imported. ChatGPT credentials grant broader account access, not a provider-enforced Dots-only scope. This bridge uses them only for authentication, Dot discovery, verified Dot room messaging and status reads for tasks attached to those rooms. Credentials are stored by the bridge runtime. Logout removes this bridge's copy, not your ChatGPT browser session. Provider session renewal is automatic while the session remains valid; reconnect if ChatGPT revokes it."

	return &bridgev2.LoginStep{Type: bridgev2.LoginStepTypeCookies, StepID: "chatgpt-dots.cookies", Instructions: instructions, CookiesParams: &bridgev2.LoginCookiesParams{
		URL: "https://chatgpt.com/auth/login",
		Fields: []bridgev2.LoginCookieField{
			{ID: "access_token", Required: true, Sources: []bridgev2.LoginCookieFieldSource{{Type: bridgev2.LoginCookieTypeSpecial, Name: "chatgpt-dots.access_token"}}, Pattern: ".+"},
			{ID: "session_token", Required: true, Sources: []bridgev2.LoginCookieFieldSource{{Type: bridgev2.LoginCookieTypeSpecial, Name: "chatgpt-dots.session_token"}}, Pattern: ".+"},
		},
		ExtractJS: `(async () => {
			if (location.origin !== 'https://chatgpt.com') return;
			for (;;) {
				const controller = new AbortController();
				const timeout = setTimeout(() => controller.abort(), 15000);
				try {
					const r = await fetch('/api/auth/session', {credentials:'include', signal:controller.signal});
					if (r.ok) {
						const s = await r.json();
						if (s.accessToken && s.sessionToken) return {access_token:s.accessToken, session_token:s.sessionToken};
					}
				} catch (_) {} finally { clearTimeout(timeout); }
				await new Promise(resolve => setTimeout(resolve, 2000));
			}
		})()`,
	}}, nil
}
func (l *Login) Cancel() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cancelled = true
	if l.cancel != nil {
		l.cancel()
	}
}
func (l *Login) SubmitCookies(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	return l.submit(ctx, chatgpt.Credentials{AccessToken: input["access_token"], SessionToken: input["session_token"]})
}
func (l *Login) submit(ctx context.Context, credentials chatgpt.Credentials) (*bridgev2.LoginStep, error) {
	if credentials.SessionToken == "" {
		return nil, errors.New("ChatGPT sign-in did not provide a renewable session; retry sign-in")
	}
	l.mu.Lock()
	if l.cancelled {
		l.mu.Unlock()
		return nil, context.Canceled
	}
	ctx, l.cancel = context.WithCancel(ctx)
	l.mu.Unlock()
	api, err := chatgpt.New(credentials)
	if err != nil {
		return nil, err
	}
	profiles, err := api.Discover(ctx)
	if err != nil {
		return nil, err
	}
	if len(profiles) != 1 {
		return nil, errors.New("expected one existing primary Dot")
	}
	key := sha256.Sum256([]byte(api.Identity.Account + "\x00" + api.Identity.User))
	loginID := networkid.UserLoginID(hex.EncodeToString(key[:]))
	l.connector.loginMu.Lock()
	defer l.connector.loginMu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	old, err := l.connector.bridge.GetExistingUserLoginByID(ctx, loginID)
	if err != nil {
		return nil, err
	}
	if old != nil && old.UserMXID != l.user.MXID {
		return nil, errors.New("this ChatGPT account is already linked to another Matrix user")
	}
	if l.connector.loggingOut[loginID] {
		if old != nil {
			return nil, errors.New("ChatGPT logout is still finishing; retry after it completes")
		}
		delete(l.connector.loggingOut, loginID)
	}
	meta := &LoginMetadata{Account: api.Identity.Account, User: api.Identity.User, Rooms: map[string]*RoomState{}}
	var oldData database.UserLogin
	linked := false
	defer func() {
		if old != nil && !linked {
			*old.UserLogin = oldData
			if err := old.Save(l.connector.bridge.BackgroundCtx); err != nil {
				old.BridgeState.Send(status.BridgeState{StateEvent: status.StateUnknownError, Message: "Could not restore the previous ChatGPT login; reconnect the original account"})
			}
			_ = l.connector.LoadUserLogin(l.connector.bridge.BackgroundCtx, old)
			old.Client.Connect(l.connector.bridge.BackgroundCtx)
		}
	}()
	if old != nil {
		old.Client.Disconnect()
		oldData = *old.UserLogin
		existing := old.Metadata.(*LoginMetadata)
		for id, state := range existing.Rooms {
			copied := *state
			copied.Overlap = append([]string(nil), state.Overlap...)
			meta.Rooms[id] = &copied
		}
		meta.Pending = make(map[string]PendingSend, len(existing.Pending))
		for id, pending := range existing.Pending {
			meta.Pending[id] = pending
		}
	}
	if len(meta.Rooms) > 1 || (len(meta.Rooms) == 1 && meta.Rooms[profiles[0].Room] == nil) {
		return nil, errors.New("the primary Dot differs from this connection; keeping the original Dot and message mappings")
	}
	for _, p := range profiles {
		room, err := api.Verify(ctx, p)
		if err != nil {
			return nil, err
		}
		state, err := roomState(api, p, room)
		if err != nil {
			return nil, err
		}
		if previous := meta.Rooms[p.Room]; previous != nil {
			if previous.Profile.ID != p.ID || previous.DotMember != state.DotMember || previous.HumanMember != state.HumanMember {
				return nil, errors.New("Dot room identity changed; refusing to reuse mappings")
			}
		} else {
			meta.Rooms[p.Room] = state
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cancelled || ctx.Err() != nil {
		return nil, context.Canceled
	}
	savedCredentials := api.Credentials()
	if err := validateCredentials(meta, savedCredentials); err != nil {
		return nil, err
	}
	meta.Credentials = &savedCredentials

	remoteName := api.Identity.Email
	if remoteName == "" {
		remoteName = "ChatGPT account"
	}
	login, err := l.user.NewLogin(ctx, &database.UserLogin{ID: loginID, RemoteName: remoteName, RemoteProfile: status.RemoteProfile{Email: api.Identity.Email}, Metadata: meta}, nil)
	if err != nil {
		return nil, errors.New("could not save ChatGPT login")
	}
	linked = true
	login.Client.Connect(l.connector.bridge.BackgroundCtx)

	return &bridgev2.LoginStep{Type: bridgev2.LoginStepTypeComplete, StepID: "chatgpt-dots.complete", Instructions: "Connected the existing primary Dot. Only messages after linking are bridged.", CompleteParams: &bridgev2.LoginCompleteParams{UserLoginID: login.ID, UserLogin: login}}, nil
}
func roomState(api *chatgpt.Client, p chatgpt.Profile, r *chatgpt.Room) (*RoomState, error) {
	state := &RoomState{Profile: p, Linked: time.Now()}
	for _, m := range r.Members {
		if m.Aeon == p.ID {
			state.DotMember = m.ID
		} else if m.Aeon == "" {
			if state.HumanMember != "" {
				return nil, errors.New("Dot room has multiple human members")
			}
			state.HumanMember = m.ID
			state.HumanName = m.Name
		}
	}
	if state.DotMember == "" || state.HumanMember == "" || len(r.Members) != 2 {
		return nil, errors.New("expected a verified one-to-one Dot room")
	}
	var latest time.Time
	for _, m := range r.Latest {
		if m.ID != "" && (state.Baseline == "" || m.Created.After(latest)) {
			state.Baseline = m.ID
			latest = m.Created
		}
	}
	state.Cursor = state.Baseline
	state.Overlap = []string{state.Baseline}
	state.Linked = latest
	if state.Baseline == "" {
		return nil, errors.New("Dot room has no baseline message ID; send a message in ChatGPT before linking")
	}
	if latest.IsZero() {
		return nil, errors.New("baseline message lacks a timestamp; cannot establish safe history boundary")
	}
	if state.HumanMember != api.Identity.User {
		return nil, errors.New("room human member does not match token user; workspace account-user mapping is not verified")
	}
	return state, nil
}
