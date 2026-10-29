package connector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/event"
)

func TestIncomingQuotedReply(t *testing.T) {
	var message chatgpt.Message
	if err := json.Unmarshal([]byte(`{"id":"reply","account_user_id":"dot","content":{"text":"An answer"},"reply_to":{"message_id":"original"},"reply_root_message_id":"root"}`), &message); err != nil {
		t.Fatal(err)
	}
	client := &Client{connector: &Connector{bridge: &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{CommandPrefix: "!dots"}}}}
	converted, err := client.convertIncoming(context.Background(), &RoomState{DotMember: "dot"}, nil, nil, message, nil)
	if err != nil {
		t.Fatal(err)
	}
	if converted.ReplyTo == nil || converted.ReplyTo.MessageID != "original" {
		t.Fatalf("missing native reply target: %+v", converted.ReplyTo)
	}
	if converted.ThreadRoot == nil || *converted.ThreadRoot != "root" {
		t.Fatalf("missing native reply root: %+v", converted.ThreadRoot)
	}
	if len(converted.Parts) != 1 || converted.Parts[0].Content.Body != "An answer" {
		t.Fatalf("reply changed the message: %+v", converted.Parts)
	}
}

func TestOutgoingReplyReconcilesWithoutResending(t *testing.T) {
	posts := 0
	var accepted chatgpt.Message
	transport := confirmationHTTP(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/backend-api/messaging/rooms/room" {
			return interactionRoomResponse(r), nil
		}
		if r.URL.Path != "/backend-api/messaging/rooms/room/messages" {
			t.Fatalf("unexpected route %s", r.URL)
		}
		if r.Method == http.MethodPost {
			posts++
			var body struct {
				RequestID string               `json:"request_id"`
				ReplyTo   *chatgpt.ReplyTarget `json:"reply_to"`
				Content   struct {
					Text string `json:"text"`
				} `json:"content"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.ReplyTo == nil || body.ReplyTo.MessageID != "original" || body.Content.Text != "An answer" {
				t.Fatalf("reply became a standalone message: %+v", body)
			}
			accepted = chatgpt.Message{ID: "accepted", Sender: "human", Created: time.Now(), RequestID: body.RequestID, ReplyTo: body.ReplyTo, ReplyRootMessageID: "root"}
			return nil, errors.New("synthetic response loss after acceptance")
		}
		response := chatgpt.Page{}
		if r.URL.Query().Get("around") == "original" {
			response.Items = []chatgpt.Message{{ID: "original", ReplyRootMessageID: "root"}}
		} else if r.URL.Query().Get("reply_root_message_id") == "root" {
			switch r.URL.Query().Get("before") {
			case "":
				response.Prev = "older-page"
			case "older-page":
				response.Items = []chatgpt.Message{accepted}
			default:
				t.Fatalf("unexpected reply cursor %s", r.URL)
			}
		} else {
			t.Fatalf("uncertain reply wasn't looked up in its native thread: %s", r.URL)
		}
		body, _ := json.Marshal(response)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(body))), Request: r}, nil
	})
	client, portal, target := interactionClient(t, transport)
	message := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: portal, Content: &event.MessageEventContent{MsgType: event.MsgText, Body: "An answer"}, Event: &event.Event{ID: "$reply", Sender: "@owner:test", Timestamp: time.Now().UnixMilli()}}, ReplyTo: target}
	if _, err := client.HandleMatrixMessage(context.Background(), message); err == nil || posts != 1 {
		t.Fatalf("response loss wasn't retained as uncertain: %v posts=%d", err, posts)
	}
	saved, err := client.connector.bridge.DB.UserLogin.GetByID(context.Background(), client.login.ID)
	if err != nil {
		t.Fatal(err)
	}
	oldAPI, login, connector := client.api, client.login, client.connector
	login.Metadata = saved.Metadata
	if err = connector.LoadUserLogin(context.Background(), login); err != nil {
		t.Fatal(err)
	}
	client = login.Client.(*Client)
	client.api, client.runContext = oldAPI, context.Background()
	response, err := client.HandleMatrixMessage(context.Background(), message)
	if err != nil || response == nil || response.DB.ID != "accepted" || response.DB.ThreadRoot != "root" || posts != 1 {
		t.Fatalf("uncertain reply was resent or not reconciled: %+v %v posts=%d", response, err, posts)
	}
	other := *target
	other.ID = "different-target"
	message.ReplyTo = &other
	if _, err = client.HandleMatrixMessage(context.Background(), message); err == nil || posts != 1 {
		t.Fatal("retry reused a request ID for another reply target")
	}
	other.Room.Receiver = "other-account"
	if _, err = client.HandleMatrixMessage(context.Background(), message); err == nil || posts != 1 {
		t.Fatal("reply crossed the account boundary")
	}
}
