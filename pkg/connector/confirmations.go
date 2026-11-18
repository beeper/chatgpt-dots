package connector

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type ConfirmationAction struct {
	EventID id.EventID `json:"event_id"`
	Action  string     `json:"action"`
	Applied bool       `json:"applied"`
}

const confirmationRenderingVersion = 3

var confirmationPersistence = map[string]string{"accept-conversation": "session", "accept-always": "always"}

func confirmationQuestion(m chatgpt.Message) string {
	text := m.Content.Text
	if summary := m.Content.Elicitation.Summary(); summary != "" && summary != text {
		if text != "" {
			text += "\n\n"
		}
		text += summary
	}
	return text
}

func confirmationText(m chatgpt.Message, prefix string) string {
	e := m.Content.Elicitation
	if e == nil || m.Deleted != nil {
		return m.Content.Text
	}
	text := confirmationQuestion(m)
	if text != "" {
		text += "\n\n"
	}
	if e.Status != "" && e.Status != "pending" {
		return text + "Confirmation status: " + e.Status
	}
	if !e.Binary() {
		return text + "This request needs the ChatGPT interface: https://chatgpt.com/dots/home"
	}
	code := e.Definition()[:12]
	if persist, ok := e.ToolApproval(); ok {
		text += fmt.Sprintf("To allow once, send `%s accept %s` in this chat.", prefix, code)
		if slices.Contains(persist, "session") {
			text += fmt.Sprintf(" To allow this tool for this conversation, send `%s accept-conversation %s`.", prefix, code)
		}
		if slices.Contains(persist, "always") {
			text += fmt.Sprintf(" To always allow this tool, send `%s accept-always %s`.", prefix, code)
		}
		return text + fmt.Sprintf(" To deny, send `%s decline %s`. Check status with `%s check %s`. You do not need to reply to this message.", prefix, code, prefix, code)
	}
	return text + fmt.Sprintf("To accept, send `%s accept %s` in this chat. To decline, send `%s decline %s`. Check status with `%s check %s`. You do not need to reply to this message.", prefix, code, prefix, code, prefix, code)
}

type pollAnswer struct {
	ID    string
	Label string
}

func confirmationPollAnswers(e *chatgpt.Elicitation) []pollAnswer {
	if e == nil || e.Status != "" && e.Status != "pending" || !e.Binary() {
		return nil
	}
	persist, ok := e.ToolApproval()
	if !ok {
		return []pollAnswer{{"accept", "Accept"}, {"decline", "Decline"}}
	}
	answers := []pollAnswer{{"accept", "Allow"}}
	if len(persist) > 0 {
		answers[0].Label = "Allow once"
	}
	if slices.Contains(persist, "session") {
		answers = append(answers, pollAnswer{"accept-conversation", "Allow this conversation"})
	}
	if slices.Contains(persist, "always") {
		answers = append(answers, pollAnswer{"accept-always", "Always allow"})
	}
	return append(answers, pollAnswer{"decline", "Deny"})
}

func confirmationPoll(question string, answers []pollAnswer) map[string]any {
	options := make([]map[string]any, len(answers))
	for i, answer := range answers {
		options[i] = map[string]any{"id": answer.ID, "org.matrix.msc1767.text": answer.Label}
	}
	return map[string]any{"org.matrix.msc3381.poll.start": map[string]any{
		"kind":           "org.matrix.msc3381.poll.disclosed",
		"max_selections": 1,
		"question":       map[string]any{"org.matrix.msc1767.text": question},
		"answers":        options,
	}}
}

func isPollVoteOrEnd(part *database.Message) bool {
	return part.PartID == pollEndPart || strings.HasPrefix(string(part.PartID), "vote:")
}

const pollEndPart networkid.PartID = "poll-end"

const maxPollQuestionBytes = 4000

func pollEndEdit(msg *simplevent.Message[chatgpt.Message], m chatgpt.Message, existing []*database.Message) bridgev2.RemoteEvent {
	if m.Deleted != nil {
		return nil
	}
	var poll *database.Message
	ts := m.Updated
	for _, part := range existing {
		if part.PartID == pollEndPart {
			return nil
		} else if md, ok := part.Metadata.(*MessageMetadata); ok && len(md.PollOptions) > 0 {
			poll = part
		} else if isPollVoteOrEnd(part) && !part.Timestamp.Before(ts) {
			ts = part.Timestamp.Add(time.Millisecond)
		}
	}
	e := m.Content.Elicitation
	if poll == nil || len(confirmationPollAnswers(e)) > 0 && e.Definition() == poll.Metadata.(*MessageMetadata).Confirmation {
		return nil
	}
	text := "This confirmation changed; check it in ChatGPT"
	if e != nil && e.Status != "" && e.Status != "pending" {
		text = "Confirmation status: " + e.Status
	}
	end := *msg
	end.Type, end.PostHandleFunc, end.Timestamp = bridgev2.RemoteEventEdit, nil, ts
	end.ConvertEditFunc = func(context.Context, *bridgev2.Portal, bridgev2.MatrixAPI, []*database.Message, chatgpt.Message) (*bridgev2.ConvertedEdit, error) {
		return &bridgev2.ConvertedEdit{AddedParts: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{ID: pollEndPart, Type: event.EventUnstablePollEnd, Content: &event.MessageEventContent{Body: text, RelatesTo: &event.RelatesTo{Type: event.RelReference, EventID: poll.MXID}}, Extra: map[string]any{"org.matrix.msc3381.poll.end": map[string]any{}, "org.matrix.msc1767.text": text}, DBMetadata: &MessageMetadata{Updated: m.Updated}}}}}, nil
	}
	return &end
}

func (c *Connector) registerConfirmationCommands() error {
	processor, ok := c.bridge.Commands.(*commands.Processor)
	if !ok {
		return errors.New("Dot confirmations require the standard bridge command processor")
	}
	for name, description := range map[string]string{
		"accept":              "accept it",
		"accept-conversation": "allow its tool for the rest of the ChatGPT conversation",
		"accept-always":       "always allow its tool",
		"decline":             "decline it",
		"check":               "check its status",
	} {
		processor.AddHandlers(&commands.FullHandler{Name: name, RequiresPortal: true, RequiresLogin: true,
			Help: commands.HelpMeta{Section: commands.HelpSectionMisc, Args: "<confirmation code>", Description: "Use the code on a Dot confirmation to " + description + "."}, Func: c.handleConfirmationCommand})
	}
	return nil
}

func (c *Connector) handleConfirmationCommand(ce *commands.Event) {
	message, uncertain, err := c.runConfirmationCommand(ce)
	if err != nil {
		message = err.Error()
		if uncertain {
			message = "ChatGPT has not confirmed this decision. It will not be sent again automatically. Use the check command shown on the original confirmation, or check it in ChatGPT."
		}
		*ce.MessageStatus = bridgev2.WrapErrorInStatus(errors.New(message)).WithMessage(message).WithIsCertain(!uncertain).WithSendNotice(false).WithStatus(event.MessageStatusFail)
	}
	ce.ReplyAdvanced(message, false, false)
}

func (c *Connector) runConfirmationCommand(ce *commands.Event) (string, bool, error) {
	if ce.Sudo || ce.User == nil || ce.Portal == nil || ce.EventID == "" || ce.OrigRoomID != ce.RoomID || ce.RoomID != ce.Portal.MXID {
		return "", false, errors.New("Use the confirmation command shown on the card in this conversation")
	}
	login, err := c.bridge.GetExistingUserLoginByID(ce.Ctx, ce.Portal.Receiver)
	if err != nil || login == nil || login.UserMXID != ce.User.MXID {
		return "", false, errors.New("This confirmation belongs to another account")
	}
	client, ok := login.Client.(*Client)
	if !ok {
		return "", false, errors.New("This is not a ChatGPT Dots conversation")
	}
	return client.answerConfirmation(ce.Ctx, ce.Portal, ce.EventID, ce.Command, func(ctx context.Context) (*database.Message, error) {
		return c.confirmationTarget(ctx, ce)
	})
}

func (c *Client) answerConfirmation(parent context.Context, portal *bridgev2.Portal, eventID id.EventID, command string, findTarget func(context.Context) (*database.Message, error)) (string, bool, error) {
	c.waitForConnect(parent)
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.mu.RLock()
	api, run := c.api, c.runContext
	room := c.meta.Rooms[string(portal.ID)]
	c.mu.RUnlock()
	if api == nil || run == nil || run.Err() != nil || !c.IsLoggedIn() || room == nil || c.login.Client != c {
		return "", false, errors.New("Reconnect this Dot before answering its confirmation")
	}
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()
	stop := context.AfterFunc(run, cancel)
	defer stop()
	target, err := findTarget(ctx)
	if err != nil || target == nil || target.Room != c.key(room) || target.SenderID != c.dotID(room) || target.HasFakeMXID() {
		return "", false, errors.New("Use the exact confirmation command shown on the card in this conversation")
	}
	metadata, ok := target.Metadata.(*MessageMetadata)
	if !ok || metadata.Confirmation == "" {
		return "", false, errors.New("This message is not a synchronized confirmation; wait for its updated instructions or open ChatGPT")
	}
	verified, err := api.Verify(ctx, room.Profile)
	if err != nil {
		return "", false, err
	}
	current, err := roomMembers(api, room.Profile, verified)
	if err != nil || current.DotMember != room.DotMember || current.HumanMember != room.HumanMember {
		return "", false, errors.New("The Dot room identity changed; refusing confirmation")
	}
	observed, err := api.ReadElicitation(ctx, room.Profile.Room, string(target.ID))
	if err != nil {
		return "", false, err
	}
	if observed.Elicitation.Definition() != metadata.Confirmation {
		return "", false, errors.New("This confirmation changed; use the updated card or open ChatGPT")
	}
	key := room.Profile.Room + "\x00" + string(target.ID) + "\x00" + metadata.Confirmation
	c.mu.RLock()
	previous, reserved := c.meta.Confirmations[key]
	c.mu.RUnlock()
	if command == "check" {
		if reserved && previous.Applied {
			return "ChatGPT confirmed this decision: " + previous.Action + ".", false, nil
		}
		if reserved {
			return "ChatGPT reports this confirmation as " + observed.Runtime + ". The previous decision remains unconfirmed; it will not be submitted again automatically.", false, nil
		}
		return "ChatGPT reports this confirmation as " + observed.Runtime + ".", false, nil
	}
	persist, remembered := confirmationPersistence[command]
	if command != "accept" && command != "decline" && !remembered {
		return "", false, errors.New("Unknown confirmation command")
	}
	action := command
	if remembered {
		action = "accept"
	}
	if reserved {
		if previous.Action == command && previous.Applied {
			return "ChatGPT already confirmed this decision: " + previous.Action + ".", false, nil
		}
		return "", !previous.Applied, errors.New("A decision was already submitted for this confirmation; check its status in ChatGPT")
	}
	if !observed.Elicitation.Binary() {
		return "", false, errors.New("This request needs the ChatGPT interface: https://chatgpt.com/dots/home")
	}
	if offered, _ := observed.Elicitation.ToolApproval(); remembered && !slices.Contains(offered, persist) {
		return "", false, errors.New("This confirmation does not offer that option; use a command shown on the card")
	}
	if !observed.CanRespond || observed.Runtime != "pending" || observed.Elicitation.Status != "" && observed.Elicitation.Status != "pending" {
		c.requestRefresh(room.Profile.Room, string(target.ID), true)
		return "", false, errors.New("This confirmation is no longer waiting for a response; check it in ChatGPT")
	}
	if err = ctx.Err(); err != nil {
		return "", false, err
	}
	// Persist before the external side effect. Ambiguous results can only be
	// inspected, never automatically re-submitted after a retry or restart.
	c.mu.Lock()
	if c.meta.Confirmations == nil {
		c.meta.Confirmations = make(map[string]ConfirmationAction)
	}
	operation := ConfirmationAction{EventID: eventID, Action: command}
	c.meta.Confirmations[key] = operation
	err = c.login.Save(ctx)
	if err != nil {
		delete(c.meta.Confirmations, key)
	}
	c.mu.Unlock()
	if err != nil {
		return "", false, errors.New("Could not save the confirmation decision; nothing submitted")
	}
	applied, err := api.RespondElicitation(ctx, room.Profile.Room, string(target.ID), observed.Fingerprint, action, persist, observed.Elicitation)
	if err != nil || !applied {
		return "", true, errors.New("ChatGPT did not confirm this decision")
	}
	c.mu.Lock()
	operation.Applied = true
	c.meta.Confirmations[key] = operation
	err = c.login.Save(ctx)
	if err != nil {
		operation.Applied = false
		c.meta.Confirmations[key] = operation
	}
	c.mu.Unlock()
	if err != nil {
		return "", true, errors.New("Could not save ChatGPT's confirmation acknowledgement")
	}
	c.requestRefresh(room.Profile.Room, string(target.ID), true)
	return "ChatGPT confirmed this decision: " + command + ".", false, nil
}

func (c *Connector) confirmationTarget(ctx context.Context, ce *commands.Event) (*database.Message, error) {
	code := strings.TrimSpace(ce.RawArgs)
	if code == "" && ce.ReplyTo != "" {
		return c.bridge.DB.Message.GetPartByMXID(ctx, ce.ReplyTo)
	}
	if len(code) != 12 || strings.Trim(code, "0123456789abcdef") != "" {
		return nil, errors.New("invalid confirmation code")
	}
	rows, err := c.bridge.DB.Message.GetDB().Query(ctx, `SELECT MIN(rowid) FROM message WHERE bridge_id=$1 AND room_id=$2 AND room_receiver=$3 AND metadata->>'confirmation' LIKE $4 GROUP BY id LIMIT 2`, c.bridge.ID, ce.Portal.ID, ce.Portal.Receiver, code+"%")
	if err != nil {
		return nil, err
	}
	var rowIDs []int64
	for rows.Next() {
		var rowID int64
		if err = rows.Scan(&rowID); err != nil {
			break
		}
		rowIDs = append(rowIDs, rowID)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil || len(rowIDs) != 1 {
		return nil, errors.New("confirmation code is missing or ambiguous")
	}
	target, err := c.bridge.DB.Message.GetByRowID(ctx, rowIDs[0])
	if err != nil || target == nil || ce.ReplyTo == "" {
		return target, err
	}
	replied, err := c.bridge.DB.Message.GetPartByMXID(ctx, ce.ReplyTo)
	if err != nil {
		return nil, err
	}
	if replied != nil && replied.ID != target.ID {
		if metadata, ok := replied.Metadata.(*MessageMetadata); ok && metadata.Confirmation != "" {
			return nil, errors.New("confirmation code does not match the replied-to card")
		}
	}
	return target, nil
}

func (c *Client) HandleMatrixPollStart(context.Context, *bridgev2.MatrixPollStart) (*bridgev2.MatrixMessageResponse, error) {
	return nil, bridgev2.ErrPollsNotSupported
}

func (c *Client) HandleMatrixPollVote(ctx context.Context, msg *bridgev2.MatrixPollVote) (*bridgev2.MatrixMessageResponse, error) {
	message, uncertain, err := c.runConfirmationVote(ctx, msg)
	if err == nil {
		return &bridgev2.MatrixMessageResponse{DB: &database.Message{ID: msg.VoteTo.ID, PartID: networkid.PartID("vote:" + msg.Event.ID), SenderID: c.GetUserID(), SenderMXID: msg.VoteTo.SenderMXID, Metadata: &MessageMetadata{}}}, nil
	}
	message = err.Error()
	if uncertain {
		message = "ChatGPT has not confirmed this decision. It will not be sent again automatically. Check it in ChatGPT."
	}
	return nil, bridgev2.WrapErrorInStatus(errors.New(message)).WithMessage(message).WithIsCertain(!uncertain).WithSendNotice(true).WithStatus(event.MessageStatusFail)
}

func (c *Client) runConfirmationVote(ctx context.Context, msg *bridgev2.MatrixPollVote) (string, bool, error) {
	metadata, _ := msg.VoteTo.Metadata.(*MessageMetadata)
	answers := msg.Content.Response.Answers
	if metadata == nil || len(metadata.PollOptions) == 0 || isPollVoteOrEnd(msg.VoteTo) {
		return "", false, errors.New("This poll can't be answered from Beeper")
	} else if len(answers) != 1 || !slices.Contains(metadata.PollOptions, answers[0]) {
		return "", false, errors.New("Choose one option to answer this confirmation")
	} else if msg.Portal.Receiver != c.login.ID || msg.Event.Sender != c.login.UserMXID {
		return "", false, errors.New("This confirmation belongs to another account")
	}
	ended, err := c.connector.bridge.DB.Message.GetPartByID(ctx, c.login.ID, msg.VoteTo.ID, pollEndPart)
	if err != nil {
		return "", false, err
	} else if ended != nil {
		return "", false, errors.New("This confirmation is no longer waiting for a response; check it in ChatGPT")
	}
	return c.answerConfirmation(ctx, msg.Portal, msg.Event.ID, answers[0], func(context.Context) (*database.Message, error) {
		return msg.VoteTo, nil
	})
}

var _ bridgev2.PollHandlingNetworkAPI = (*Client)(nil)

func (c *Client) refreshOpenPolls(ctx context.Context, state *RoomState) error {
	rows, err := c.connector.bridge.DB.Message.GetDB().Query(ctx, `SELECT id FROM message p WHERE bridge_id=$1 AND room_id=$2 AND room_receiver=$3 AND metadata->>'poll_options' IS NOT NULL AND NOT EXISTS (SELECT 1 FROM message e WHERE e.bridge_id=p.bridge_id AND e.room_receiver=p.room_receiver AND e.id=p.id AND e.part_id=$4)`, c.connector.bridge.ID, state.Profile.Room, c.login.ID, pollEndPart)
	if err != nil {
		return err
	}
	defer rows.Close()
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return err
		}
		if c.refresh == nil {
			c.refresh = make(map[string]map[string]bool)
		}
		if c.refresh[state.Profile.Room] == nil {
			c.refresh[state.Profile.Room] = make(map[string]bool)
		}
		c.refresh[state.Profile.Room][id] = true
	}
	return rows.Err()
}
