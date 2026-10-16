package connector

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
)

type confirmationMatrix struct{ bridgev2.MatrixConnector }

func (*confirmationMatrix) Init(*bridgev2.Bridge)         {}
func (*confirmationMatrix) BotIntent() bridgev2.MatrixAPI { return nil }

type confirmationHTTP func(*http.Request) (*http.Response, error)

func (f confirmationHTTP) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestConfirmationCommandPersistenceAndOwnership(t *testing.T) {
	for _, outcome := range []string{"applied", "unconfirmed", "lost-response"} {
		t.Run(outcome, func(t *testing.T) {
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
			var m chatgpt.Message
			if err = json.Unmarshal([]byte(`{"id":"card","account_user_id":"dot-member","content":{"text":"Save custom rule?","elicitation":{"request_id":"request","thread_id":"thread","turn_id":"turn","status":"pending","params":{"threadId":"thread","turnId":"turn","serverName":"custom_user_rules","mode":"form","message":"Save custom rule?","requestedSchema":{"type":"object","properties":{}}}}}}`), &m); err != nil {
				t.Fatal(err)
			}
			room := &RoomState{Profile: chatgpt.Profile{ID: "dot", Room: "room"}, DotMember: "dot-member", HumanMember: "human"}
			meta := &LoginMetadata{Account: "account", User: "human", Rooms: map[string]*RoomState{"room": room}}
			if err = br.DB.User.Insert(ctx, &database.User{MXID: "@owner:test"}); err != nil {
				t.Fatal(err)
			}
			if err = br.DB.UserLogin.Insert(ctx, &database.UserLogin{ID: "login", UserMXID: "@owner:test", Metadata: meta}); err != nil {
				t.Fatal(err)
			}
			login, err := br.GetExistingUserLoginByID(ctx, "login")
			if err != nil || login == nil {
				t.Fatalf("load login: %v", err)
			}
			client := login.Client.(*Client)
			portal := &database.Portal{PortalKey: client.key(room), MXID: "!room:test"}
			if err = br.DB.Portal.Insert(ctx, portal); err != nil {
				t.Fatal(err)
			}
			target := &database.Message{ID: "card", MXID: "$card", Room: client.key(room), SenderID: client.dotID(room), Timestamp: time.Now(), Metadata: &MessageMetadata{Confirmation: m.Content.Elicitation.Definition()}}
			if err = br.DB.Message.Insert(ctx, target); err != nil {
				t.Fatal(err)
			}
			posts := 0
			fingerprint := "fresh"
			transport := confirmationHTTP(func(r *http.Request) (*http.Response, error) {
				body := `{"type":"DM","app_source":"chatgpt:messaging","id":"room","aeon_id":"dot","members":[{"account_user_id":"dot-member","aeon_id":"dot"},{"account_user_id":"human"}],"latest_messages":[{"id":"baseline","created_at":"2026-10-16T00:00:00Z"}]}`
				if strings.HasSuffix(r.URL.Path, "/elicitation") {
					b, _ := json.Marshal(chatgpt.ElicitationState{Elicitation: m.Content.Elicitation, Fingerprint: fingerprint, CanRespond: true, Runtime: "pending"})
					body = string(b)
				} else if strings.HasSuffix(r.URL.Path, "/response") {
					posts++
					// Verify that the reservation is already durable when the POST begins.
					saved, e := br.DB.UserLogin.GetByID(ctx, "login")
					if e != nil || len(saved.Metadata.(*LoginMetadata).Confirmations) != 1 {
						t.Fatal("POST preceded durable reservation")
					}
					if outcome == "lost-response" {
						return nil, errors.New("synthetic connection reset")
					}
					body = `{"status":"` + outcome + `"}`
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			claims := `{"exp":4102444800,"https://api.openai.com/auth":{"chatgpt_account_id":"account","chatgpt_user_id":"human"}}`
			api, err := chatgpt.New(chatgpt.Credentials{AccessToken: "header." + base64.RawURLEncoding.EncodeToString([]byte(claims)) + ".signature"}, transport)
			if err != nil {
				t.Fatal(err)
			}
			client.api, client.runContext = api, ctx
			client.connected.Store(true)
			user := &bridgev2.User{User: &database.User{MXID: "@owner:test"}}
			ce := &commands.Event{Ctx: ctx, Portal: &bridgev2.Portal{Portal: portal}, User: user, RoomID: portal.MXID, OrigRoomID: portal.MXID, EventID: "$decision", ReplyTo: target.MXID, Command: "accept"}
			m.Content.Elicitation.Params.Message = "A changed rule"
			if _, _, err = connector.runConfirmationCommand(ce); err == nil || posts != 0 {
				t.Fatal("stale card reached provider submission")
			}
			m.Content.Elicitation.Params.Message = "Save custom rule?"
			ce.ReplyTo = ""
			if _, _, err = connector.runConfirmationCommand(ce); err == nil || posts != 0 {
				t.Fatal("unquoted command reached provider submission")
			}
			ce.ReplyTo = target.MXID
			_, uncertain, err := connector.runConfirmationCommand(ce)
			if posts != 1 || (outcome == "applied") != (err == nil) || uncertain != (outcome != "applied") {
				t.Fatalf("posts=%d uncertain=%v err=%v", posts, uncertain, err)
			}
			// Reload persisted login metadata as a restarted worker would, rotate the
			// provider fingerprint, and prove neither replay nor reversal re-submits.
			saved, err := br.DB.UserLogin.GetByID(ctx, "login")
			if err != nil {
				t.Fatal(err)
			}
			login.Metadata = saved.Metadata
			client.meta = saved.Metadata.(*LoginMetadata)
			fingerprint = "rotated"
			ce.EventID = "$retry"
			_, _, _ = connector.runConfirmationCommand(ce)
			ce.Command = "decline"
			_, _, _ = connector.runConfirmationCommand(ce)
			ce.Command = "check"
			_, _, _ = connector.runConfirmationCommand(ce)
			if posts != 1 {
				t.Fatal("retry, reversal or check re-submitted decision")
			}
			ce.User = &bridgev2.User{User: &database.User{MXID: "@other:test"}}
			if _, _, err = connector.runConfirmationCommand(ce); err == nil {
				t.Fatal("another account could inspect confirmation")
			}
			if !client.connected.Load() {
				t.Fatal("confirmation failure disconnected account")
			}
		})
	}
}

func TestConfirmationRenderingAndRevisions(t *testing.T) {
	var m chatgpt.Message
	_ = json.Unmarshal([]byte(`{"id":"card","account_user_id":"dot-member","content":{"elicitation":{"request_id":"request","thread_id":"thread","params":{"threadId":"thread","message":"Save rule?","requestedSchema":{"type":"object","properties":{}}}}}}`), &m)
	c := &Client{connector: &Connector{bridge: &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{CommandPrefix: "!custom"}}}}
	converted, err := c.convertIncoming(context.Background(), &RoomState{DotMember: "dot-member"}, nil, nil, m, nil)
	if err != nil || len(converted.Parts) != 1 {
		t.Fatalf("missing empty-text confirmation: %v", err)
	}
	if !strings.Contains(converted.Parts[0].Content.Body, "!custom accept") || converted.Parts[0].DBMetadata.(*MessageMetadata).Confirmation == "" {
		t.Fatal("card has no actionable instructions or identity")
	}
	before := revision(m)
	m.Content.Elicitation.Params.Message = "Different rule?"
	if before == revision(m) {
		t.Fatal("changed confirmation not edited")
	}
	m.Content.Elicitation.Status = "resolved"
	if strings.Contains(confirmationText(m, "!dots"), "!dots accept") {
		t.Fatal("resolved card still offers acceptance")
	}
}
