package connector

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
)

func interactionClient(t *testing.T, transport http.RoundTripper) (*Client, *bridgev2.Portal, *database.Message) {
	t.Helper()
	ctx := context.Background()
	sqldb, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqldb.Close() })
	db, err := dbutil.NewWithDB(sqldb, "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	connector := &Connector{}
	br := bridgev2.NewBridge("test", db, zerolog.Nop(), &bridgeconfig.BridgeConfig{CommandPrefix: "!dots"}, &confirmationMatrix{}, connector, commands.NewProcessor)
	if err = br.DB.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	room := &RoomState{Profile: chatgpt.Profile{ID: "dot", Room: "room"}, DotMember: "dot-member", HumanMember: "human"}
	metadata := &LoginMetadata{Account: "account", User: "human", Rooms: map[string]*RoomState{"room": room}}
	if err = br.DB.User.Insert(ctx, &database.User{MXID: "@owner:test"}); err != nil {
		t.Fatal(err)
	}
	if err = br.DB.UserLogin.Insert(ctx, &database.UserLogin{ID: "login", UserMXID: "@owner:test", Metadata: metadata}); err != nil {
		t.Fatal(err)
	}
	login, err := br.GetExistingUserLoginByID(ctx, "login")
	if err != nil {
		t.Fatal(err)
	}
	client := login.Client.(*Client)
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: client.key(room), MXID: "!room:test"}}
	if err = br.DB.Portal.Insert(ctx, portal.Portal); err != nil {
		t.Fatal(err)
	}
	target := &database.Message{ID: "original", MXID: "$original", Room: portal.PortalKey, SenderID: client.dotID(room), Timestamp: time.Now(), Metadata: &MessageMetadata{}}
	if err = br.DB.Message.Insert(ctx, target); err != nil {
		t.Fatal(err)
	}
	claims := `{"exp":4102444800,"https://api.openai.com/auth":{"chatgpt_account_id":"account","chatgpt_user_id":"human"}}`
	client.api, err = chatgpt.New(chatgpt.Credentials{AccessToken: "header." + base64.RawURLEncoding.EncodeToString([]byte(claims)) + ".signature"}, transport)
	if err != nil {
		t.Fatal(err)
	}
	client.runContext = ctx
	return client, portal, target
}

func interactionRoomResponse(r *http.Request) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"type":"DM","app_source":"chatgpt:messaging","id":"room","aeon_id":"dot","members":[{"account_user_id":"dot-member","aeon_id":"dot"},{"account_user_id":"human"}],"latest_messages":[{"id":"baseline","created_at":"2026-10-25T00:00:00Z"}]}`)), Request: r}
}

func TestOutgoingReactionReplacementAndStaleRemoval(t *testing.T) {
	var current string
	posts, deletes := 0, 0
	transport := confirmationHTTP(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/backend-api/messaging/rooms/room" {
			return interactionRoomResponse(r), nil
		}
		if r.URL.Path == "/backend-api/messaging/rooms/room/messages" && r.URL.Query().Get("around") == "original" {
			body, _ := json.Marshal(map[string]any{"items": []any{map[string]any{"id": "original", "reactions": map[string]string{"human": current}}}})
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(body))), Request: r}, nil
		}
		if r.URL.Path != "/backend-api/messaging/rooms/room/messages/original/reactions" {
			t.Fatalf("unexpected route %s %s", r.Method, r.URL)
		}
		if r.Method == http.MethodPost {
			var body struct {
				Reaction string `json:"reaction"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			posts++
			current = body.Reaction
		} else if r.Method == http.MethodDelete {
			deletes++
			current = ""
		} else {
			t.Fatalf("unexpected method %s", r.Method)
		}
		return &http.Response{StatusCode: 204, Body: http.NoBody, Request: r}, nil
	})
	client, portal, target := interactionClient(t, transport)
	api, ok := any(client).(bridgev2.ReactionHandlingNetworkAPI)
	if !ok {
		t.Fatal("outgoing reactions are not implemented")
	}
	ctx := context.Background()
	for _, emoji := range []string{"👍", "👩🏽‍💻"} {
		msg := &bridgev2.MatrixReaction{MatrixEventBase: bridgev2.MatrixEventBase[*event.ReactionEventContent]{Portal: portal, Content: &event.ReactionEventContent{RelatesTo: event.RelatesTo{Key: emoji}}}, TargetMessage: target}
		pre, err := api.PreHandleMatrixReaction(ctx, msg)
		if err != nil || pre.MaxReactions != 1 || pre.EmojiID != "" || pre.SenderID != client.GetUserID() {
			t.Fatalf("incorrect single-reaction contract: %+v, %v", pre, err)
		}
		msg.PreHandleResp = &pre
		if _, err = api.HandleMatrixReaction(ctx, msg); err != nil || current != emoji {
			t.Fatalf("reaction was not replaced: %q %v", current, err)
		}
	}
	old := &database.Reaction{Room: target.Room, MessageID: target.ID, SenderID: client.GetUserID(), Emoji: "👍"}
	remove := &bridgev2.MatrixReactionRemove{MatrixEventBase: bridgev2.MatrixEventBase[*event.RedactionEventContent]{Portal: portal}, TargetReaction: old}
	if err := api.HandleMatrixReactionRemove(ctx, remove); err != nil || deletes != 0 || current != "👩🏽‍💻" {
		t.Fatalf("old redaction removed the newer native reaction: %q %v", current, err)
	}
	old.Emoji = current
	if err := api.HandleMatrixReactionRemove(ctx, remove); err != nil || deletes != 1 || current != "" || posts != 2 {
		t.Fatalf("current reaction was not removed: posts=%d deletes=%d current=%q err=%v", posts, deletes, current, err)
	}
	old.Room.Receiver = "other-login"
	if err := api.HandleMatrixReactionRemove(ctx, remove); err == nil || deletes != 1 {
		t.Fatal("reaction from another login reached the provider")
	}
}
