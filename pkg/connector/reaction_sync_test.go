package connector

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type reactionSyncSend struct {
	sender  id.UserID
	typeOf  event.Type
	content *event.Content
}

type reactionSyncMatrix struct {
	bridgev2.MatrixConnector
	sends []reactionSyncSend
}

func (m *reactionSyncMatrix) GhostIntent(user networkid.UserID) bridgev2.MatrixAPI {
	return &reactionSyncIntent{matrix: m, mxid: m.FormatGhostMXID(user)}
}
func (m *reactionSyncMatrix) FormatGhostMXID(user networkid.UserID) id.UserID {
	return id.UserID("@ghost_" + string(user) + ":test")
}
func (*reactionSyncMatrix) ParseGhostMXID(mxid id.UserID) (networkid.UserID, bool) {
	if strings.HasPrefix(string(mxid), "@ghost_") && strings.HasSuffix(string(mxid), ":test") {
		return networkid.UserID(strings.TrimSuffix(strings.TrimPrefix(string(mxid), "@ghost_"), ":test")), true
	}
	return "", false
}
func (m *reactionSyncMatrix) NewUserIntent(_ context.Context, user id.UserID, token string) (bridgev2.MatrixAPI, string, error) {
	return &reactionSyncIntent{matrix: m, mxid: user}, token, nil
}
func (m *reactionSyncMatrix) BotIntent() bridgev2.MatrixAPI {
	return &reactionSyncIntent{matrix: m, mxid: "@bot:test"}
}

type reactionSyncIntent struct {
	bridgev2.MatrixAPI
	matrix *reactionSyncMatrix
	mxid   id.UserID
}

func (i *reactionSyncIntent) GetMXID() id.UserID { return i.mxid }
func (*reactionSyncIntent) EnsureJoined(context.Context, id.RoomID, ...bridgev2.EnsureJoinedParams) error {
	return nil
}
func (i *reactionSyncIntent) SendMessage(_ context.Context, _ id.RoomID, typ event.Type, content *event.Content, _ *bridgev2.MatrixSendExtra) (*mautrix.RespSendEvent, error) {
	i.matrix.sends = append(i.matrix.sends, reactionSyncSend{sender: i.mxid, typeOf: typ, content: content})
	return &mautrix.RespSendEvent{EventID: id.EventID(fmt.Sprintf("$sent%d", len(i.matrix.sends)))}, nil
}

func TestNativeReactionSyncBothParticipantsAndParts(t *testing.T) {
	ctx := context.Background()
	client, portal, target := interactionClient(t, nil)
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
	filePart := *target
	filePart.PartID, filePart.MXID = "file", "$file"
	if err := br.DB.Message.Insert(ctx, &filePart); err != nil {
		t.Fatal(err)
	}
	message := chatgpt.Message{ID: string(target.ID), Created: time.Now(), Reactions: map[string]string{"dot-member": "👍", "human": "❤️"}}
	assertRows := func(want map[networkid.UserID]string, selfPart networkid.PartID) {
		t.Helper()
		rows, err := br.DB.Reaction.GetAllToMessage(ctx, client.login.ID, target.ID)
		if err != nil || len(rows) != len(want) {
			t.Fatalf("reaction rows=%+v err=%v; want %+v", rows, err, want)
		}
		for _, row := range rows {
			if row.Emoji != want[row.SenderID] || row.Room != portal.PortalKey {
				t.Fatalf("incorrect persisted reaction: %+v; want %+v", row, want)
			}
			if row.SenderID == client.GetUserID() && row.MessagePartID != selfPart {
				t.Fatalf("self reaction moved from selected attachment: %+v", row)
			}
		}
	}
	if err := client.syncReactions(ctx, state, message); err != nil {
		t.Fatal(err)
	}
	assertRows(map[networkid.UserID]string{client.dotID(state): "👍", client.GetUserID(): "❤️"}, "")
	if len(matrix.sends) != 2 || matrix.sends[0].typeOf != event.EventReaction || matrix.sends[1].typeOf != event.EventReaction || matrix.sends[1].sender != "@owner:test" {
		t.Fatalf("native reactions did not pass through Matrix I/O for both participants: %+v", matrix.sends)
	}
	self, err := br.DB.Reaction.GetByID(ctx, client.login.ID, target.ID, "", client.GetUserID(), "")
	if err != nil || self == nil {
		t.Fatalf("self reaction lookup: %+v %v", self, err)
	}
	if err = br.DB.Reaction.Delete(ctx, self); err != nil {
		t.Fatal(err)
	}
	self.MessagePartID, self.MXID = filePart.PartID, "$self-on-file"
	if err = br.DB.Reaction.Upsert(ctx, self); err != nil {
		t.Fatal(err)
	}
	message.Reactions = map[string]string{"dot-member": "🔥", "human": "👩🏽‍💻"}
	if err = client.syncReactions(ctx, state, message); err != nil {
		t.Fatal(err)
	}
	assertRows(map[networkid.UserID]string{client.dotID(state): "🔥", client.GetUserID(): "👩🏽‍💻"}, "file")
	lastReaction := matrix.sends[len(matrix.sends)-1].content.AsReaction()
	if lastReaction.RelatesTo.EventID != "$file" {
		t.Fatalf("replacement reaction targeted another Matrix part: %+v", lastReaction)
	}
	delete(message.Reactions, "dot-member")
	if err = client.syncReactions(ctx, state, message); err != nil {
		t.Fatal(err)
	}
	assertRows(map[networkid.UserID]string{client.GetUserID(): "👩🏽‍💻"}, "file")
	delete(message.Reactions, "human")
	if err = client.syncReactions(ctx, state, message); err != nil {
		t.Fatal(err)
	}
	assertRows(map[networkid.UserID]string{}, "")
}
