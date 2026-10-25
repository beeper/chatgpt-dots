package connector

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
)

func (i *reactionSyncIntent) IsDoublePuppet() bool { return i.mxid == "@owner:test" }

func TestNativeReplyUpgradeReplayDoesNotEditUnchangedText(t *testing.T) {
	for _, participant := range []string{"dot", "owner"} {
		t.Run(participant, func(t *testing.T) {
			ctx := context.Background()
			client, portal, root := interactionClient(t, nil)
			br := client.connector.bridge
			br.BackgroundCtx = ctx
			matrix := &reactionSyncMatrix{}
			br.Matrix, br.Bot = matrix, matrix.BotIntent()
			state := client.meta.Rooms["room"]
			sender, senderID := state.DotMember, client.dotID(state)
			if participant == "owner" {
				sender, senderID = state.HumanMember, client.GetUserID()
			}
			if err := br.DB.Ghost.Insert(ctx, &database.Ghost{ID: senderID, Name: "Test participant", NameSet: true, AvatarSet: true}); err != nil {
				t.Fatal(err)
			}
			message := chatgpt.Message{ID: "reply", Sender: sender, Created: time.Now()}
			message.Content.Text = "An existing answer"
			converted, err := client.convertIncoming(ctx, state, portal, nil, message, nil)
			if err != nil {
				t.Fatal(err)
			}
			senderMXID := matrix.FormatGhostMXID(senderID)
			if participant == "owner" {
				senderMXID = "@owner:test"
			}
			if err = br.DB.Message.Insert(ctx, &database.Message{ID: "reply", MXID: "$reply", Room: portal.PortalKey, SenderID: senderID, SenderMXID: senderMXID, Timestamp: message.Created, Metadata: converted.Parts[0].DBMetadata}); err != nil {
				t.Fatal(err)
			}
			message.ReplyTo = &chatgpt.ReplyTarget{MessageID: string(root.ID)}
			message.ReplyRootMessageID = string(root.ID)
			if err = client.deliver(ctx, state, message); err != nil {
				t.Fatal(err)
			}
			if len(matrix.sends) != 0 {
				t.Fatalf("unchanged native reply emitted Matrix events after upgrade: %+v", matrix.sends)
			}
			message.Content.Text = "An actual update"
			if err = client.deliver(ctx, state, message); err != nil {
				t.Fatal(err)
			}
			if len(matrix.sends) != 1 || matrix.sends[0].typeOf != event.EventMessage || matrix.sends[0].content.AsMessage().RelatesTo.GetReplaceID() != "$reply" {
				t.Fatalf("actual text update did not emit a Matrix edit: %+v", matrix.sends)
			}
		})
	}
}

func TestReplyUpdatePreservesRelationshipsOnNewParts(t *testing.T) {
	ctx := context.Background()
	client, portal, root := interactionClient(t, nil)
	br := client.connector.bridge
	br.BackgroundCtx = ctx
	matrix := &reactionSyncMatrix{}
	br.Matrix, br.Bot = matrix, matrix.BotIntent()
	state := client.meta.Rooms["room"]
	for _, sender := range []networkid.UserID{client.dotID(state), client.GetUserID()} {
		if err := br.DB.Ghost.Insert(ctx, &database.Ghost{ID: sender, Name: "Test participant", NameSet: true, AvatarSet: true}); err != nil {
			t.Fatal(err)
		}
	}
	message := chatgpt.Message{ID: "reply", Sender: state.DotMember, Created: time.Now(), ReplyTo: &chatgpt.ReplyTarget{MessageID: string(root.ID)}, ReplyRootMessageID: string(root.ID)}
	message.Content.Text = "An answer"
	converted, err := client.convertIncoming(ctx, state, portal, nil, message, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = br.DB.Message.Insert(ctx, &database.Message{ID: "reply", MXID: "$reply", Room: portal.PortalKey, SenderID: client.dotID(state), SenderMXID: matrix.FormatGhostMXID(client.dotID(state)), ThreadRoot: root.ID, Timestamp: message.Created, Metadata: converted.Parts[0].DBMetadata}); err != nil {
		t.Fatal(err)
	}
	message.Content.Attachments = []json.RawMessage{json.RawMessage(`{"type":"unsupported"}`)}
	if err = client.deliver(ctx, state, message); err != nil {
		t.Fatal(err)
	}
	parts, err := br.DB.Message.GetAllPartsByID(ctx, client.login.ID, "reply")
	if err != nil || len(parts) != 2 {
		t.Fatalf("late part did not persist: %+v, %v", parts, err)
	}
	for _, part := range parts {
		if part.ThreadRoot != root.ID {
			t.Fatalf("late reply part escaped its native thread: %+v", part)
		}
	}
	for _, sent := range matrix.sends {
		if sent.typeOf == event.EventMessage && sent.content.AsMessage().MsgType == event.MsgNotice {
			relation := sent.content.AsMessage().RelatesTo
			if relation == nil || relation.GetThreadParent() != root.MXID {
				t.Fatalf("late reply part lost its Matrix relationship: %+v", relation)
			}
			return
		}
	}
	t.Fatal("late reply part was not sent to Matrix")
}
