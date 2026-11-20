package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"github.com/google/uuid"
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
		return optionPickerText(m)
	}
	text := confirmationQuestion(m)
	if text != "" {
		text += "\n\n"
	}
	if e.Status != "" && e.Status != "pending" {
		return text + "Confirmation status: " + e.Status
	}
	if _, choices, ok := e.Choices(); ok {
		for i, choice := range choices {
			text += fmt.Sprintf("%d. %s\n", i+1, choice.Label)
		}
		return text + "\nAnswer it in the poll or in ChatGPT: https://chatgpt.com/dots/home"
	} else if !e.Binary() {
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

func choiceAnswer(i int) string {
	return "choice-" + strconv.Itoa(i)
}

func choiceIndex(answer string) (int, bool) {
	index, ok := strings.CutPrefix(answer, "choice-")
	i, err := strconv.Atoi(index)
	return i, ok && err == nil && i >= 0 && choiceAnswer(i) == answer
}

func choicePollAnswers(options []string) []pollAnswer {
	answers := make([]pollAnswer, len(options))
	for i, option := range options {
		answers[i] = pollAnswer{choiceAnswer(i), option}
	}
	return answers
}

func confirmationPollAnswers(e *chatgpt.Elicitation) []pollAnswer {
	if e == nil || e.Status != "" && e.Status != "pending" {
		return nil
	}
	if _, choices, ok := e.Choices(); ok {
		labels := make([]string, len(choices))
		for i, choice := range choices {
			labels[i] = choice.Label
		}
		return append(choicePollAnswers(labels), pollAnswer{"decline", "Decline"})
	} else if !e.Binary() {
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

const maxPollAnswers = 20

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
	if poll == nil {
		return nil
	}
	e := m.Content.Elicitation
	text := "This confirmation changed; check it in ChatGPT"
	if definition := poll.Metadata.(*MessageMetadata).OptionPicker; definition != "" {
		picker, ok := m.OptionPicker()
		if !ok || optionPickerDefinition(m, picker) != definition {
			text = "This question changed; check it in ChatGPT"
		} else if picker.Selected == nil {
			return nil
		} else {
			text = selectedOption(picker)
		}
	} else if len(confirmationPollAnswers(e)) > 0 && e.Definition() == poll.Metadata.(*MessageMetadata).Confirmation {
		return nil
	} else if e != nil && e.Status != "" && e.Status != "pending" {
		text = "Confirmation status: " + e.Status
	}
	end := *msg
	end.Type, end.PostHandleFunc, end.Timestamp = bridgev2.RemoteEventEdit, nil, ts
	end.ConvertEditFunc = func(context.Context, *bridgev2.Portal, bridgev2.MatrixAPI, []*database.Message, chatgpt.Message) (*bridgev2.ConvertedEdit, error) {
		return pollEnd(poll, text, m.Updated), nil
	}
	return &end
}

func pollEnd(poll *database.Message, text string, updated time.Time) *bridgev2.ConvertedEdit {
	return &bridgev2.ConvertedEdit{AddedParts: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{ID: pollEndPart, Type: event.EventUnstablePollEnd, Content: &event.MessageEventContent{Body: text, RelatesTo: &event.RelatesTo{Type: event.RelReference, EventID: poll.MXID}}, Extra: map[string]any{"org.matrix.msc3381.poll.end": map[string]any{}, "org.matrix.msc1767.text": text}, DBMetadata: &MessageMetadata{Updated: updated}}}}}
}

func (c *Client) endClosedPoll(room *RoomState, target *database.Message, text string) {
	evt := &simplevent.Message[chatgpt.Message]{EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventEdit, PortalKey: c.key(room), Sender: bridgev2.EventSender{Sender: c.dotID(room)}, Timestamp: time.Now()}, ID: target.ID, TargetMessage: target.ID}
	evt.ConvertEditFunc = func(_ context.Context, _ *bridgev2.Portal, _ bridgev2.MatrixAPI, existing []*database.Message, _ chatgpt.Message) (*bridgev2.ConvertedEdit, error) {
		var poll *database.Message
		for _, part := range existing {
			if part.PartID == pollEndPart {
				return &bridgev2.ConvertedEdit{}, nil
			} else if md, ok := part.Metadata.(*MessageMetadata); ok && len(md.PollOptions) > 0 {
				poll = part
			}
		}
		if poll == nil {
			return &bridgev2.ConvertedEdit{}, nil
		}
		return pollEnd(poll, text, time.Time{}), nil
	}
	go c.login.QueueRemoteEvent(evt)
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
	if !ok || metadata.Confirmation == "" && metadata.OptionPicker == "" {
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
	if metadata.OptionPicker != "" {
		return c.answerOptionPicker(ctx, api, room, target, metadata.OptionPicker, eventID, command)
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
			return "ChatGPT confirmed this decision: " + decisionName(observed.Elicitation, previous.Action) + ".", false, nil
		}
		if reserved {
			return "ChatGPT reports this confirmation as " + observed.Runtime + ". The previous decision remains unconfirmed; it will not be submitted again automatically.", false, nil
		}
		return "ChatGPT reports this confirmation as " + observed.Runtime + ".", false, nil
	}
	persist, remembered := confirmationPersistence[command]
	choice, chosen := choiceIndex(command)
	if command != "accept" && command != "decline" && !remembered && !chosen {
		return "", false, errors.New("Unknown confirmation command")
	}
	action := command
	if remembered || chosen {
		action = "accept"
	}
	if reserved {
		if previous.Action == command && previous.Applied {
			return "ChatGPT already confirmed this decision: " + decisionName(observed.Elicitation, previous.Action) + ".", false, nil
		}
		return "", !previous.Applied, errors.New("A decision was already submitted for this confirmation; check its status in ChatGPT")
	}
	_, choices, choosable := observed.Elicitation.Choices()
	if chosen && (!choosable || choice >= len(choices)) {
		return "", false, errors.New("This confirmation does not offer that option; use a command shown on the card")
	} else if !observed.Elicitation.Binary() && !(choosable && (chosen || command == "decline")) {
		return "", false, errors.New("This request needs the ChatGPT interface: https://chatgpt.com/dots/home")
	}
	if offered, _ := observed.Elicitation.ToolApproval(); remembered && !slices.Contains(offered, persist) {
		return "", false, errors.New("This confirmation does not offer that option; use a command shown on the card")
	}
	if !observed.CanRespond || observed.Runtime != "pending" || observed.Elicitation.Status != "" && observed.Elicitation.Status != "pending" {
		if observed.Runtime != "pending" {
			c.endClosedPoll(room, target, "Confirmation status: "+observed.Runtime)
		}
		c.requestRefresh(room.Profile.Room, string(target.ID), true)
		return "", false, errors.New("This confirmation is no longer waiting for a response; check it in ChatGPT")
	}
	if err = ctx.Err(); err != nil {
		return "", false, err
	}
	uncertain, err := c.submitDecision(ctx, key, ConfirmationAction{EventID: eventID, Action: command}, func() (bool, error) {
		if chosen {
			return api.RespondElicitationChoice(ctx, room.Profile.Room, string(target.ID), observed.Fingerprint, choice, observed.Elicitation)
		}
		return api.RespondElicitation(ctx, room.Profile.Room, string(target.ID), observed.Fingerprint, action, persist, observed.Elicitation)
	})
	if err != nil {
		return "", uncertain, err
	}
	c.requestRefresh(room.Profile.Room, string(target.ID), true)
	return "ChatGPT confirmed this decision: " + decisionName(observed.Elicitation, command) + ".", false, nil
}

func decisionName(e *chatgpt.Elicitation, command string) string {
	if i, ok := choiceIndex(command); ok {
		if _, choices, ok := e.Choices(); ok && i < len(choices) {
			return choices[i].Label
		}
	}
	return command
}

func (c *Client) submitDecision(ctx context.Context, key string, operation ConfirmationAction, submit func() (bool, error)) (bool, error) {
	// Persist before the external side effect. Ambiguous results can only be
	// inspected, never automatically re-submitted after a retry or restart.
	c.mu.Lock()
	if c.meta.Confirmations == nil {
		c.meta.Confirmations = make(map[string]ConfirmationAction)
	}
	c.meta.Confirmations[key] = operation
	err := c.login.Save(ctx)
	if err != nil {
		delete(c.meta.Confirmations, key)
	}
	c.mu.Unlock()
	if err != nil {
		return false, errors.New("Could not save the confirmation decision; nothing submitted")
	}
	applied, err := submit()
	if err != nil || !applied {
		return true, errors.New("ChatGPT did not confirm this decision")
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
		return true, errors.New("Could not save ChatGPT's confirmation acknowledgement")
	}
	return false, nil
}

func optionPickerDefinition(m chatgpt.Message, picker *chatgpt.OptionPicker) string {
	encoded, _ := json.Marshal([]any{m.Content.Text, picker.Options})
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:])
}

func optionPickerPoll(m chatgpt.Message) ([]pollAnswer, string) {
	picker, ok := m.OptionPicker()
	if !ok || picker.Selected != nil || m.Content.Elicitation != nil {
		return nil, ""
	}
	return choicePollAnswers(picker.Options), optionPickerDefinition(m, picker)
}

func optionPickerText(m chatgpt.Message) string {
	picker, ok := m.OptionPicker()
	if !ok || m.Deleted != nil || m.Content.Elicitation != nil {
		return m.Content.Text
	}
	text := m.Content.Text
	for i, option := range picker.Options {
		if i == 0 && text != "" {
			text += "\n"
		}
		text += fmt.Sprintf("\n%d. %s", i+1, option)
	}
	return text
}

func selectedOption(picker *chatgpt.OptionPicker) string {
	if picker.Selected == nil || *picker.Selected < 0 {
		return "This question was closed in ChatGPT"
	}
	return "Answered: " + picker.Options[*picker.Selected]
}

func (c *Client) optionPickerOutcome(ctx context.Context, api *chatgpt.Client, state *RoomState, m *chatgpt.Message, picker *chatgpt.OptionPicker) (string, error) {
	if picker.Selected != nil {
		return selectedOption(picker), nil
	}
	root := m.ReplyRootMessageID
	if root == "" {
		root = m.ID
	}
	before := ""
	seen := make(map[string]bool)
	for !seen[before] {
		seen[before] = true
		page, err := api.Replies(ctx, state.Profile.Room, root, before)
		if err != nil {
			return "", err
		}
		for _, reply := range page.Items {
			if _, ok := reply.OptionPickerSelection(); ok && reply.Deleted == nil && reply.Sender == state.HumanMember && reply.ReplyTo.MessageID == m.ID {
				return "Answered: " + reply.Content.Text, nil
			}
		}
		if page.Prev == "" {
			return "", nil
		}
		before = page.Prev
	}
	return "", errors.New("reply pagination repeated; refusing another answer")
}

func (c *Client) refreshOpenPicker(ctx context.Context, api *chatgpt.Client, state *RoomState, id, definition string) (bool, error) {
	current, err := api.Message(ctx, state.Profile.Room, id)
	if err != nil {
		return false, err
	}
	picker, ok := current.OptionPicker()
	if !ok || current.Deleted != nil || optionPickerDefinition(*current, picker) != definition {
		return false, nil
	}
	outcome, err := c.optionPickerOutcome(ctx, api, state, current, picker)
	if err == nil && outcome != "" {
		c.endClosedPoll(state, &database.Message{ID: networkid.MessageID(id)}, outcome)
	}
	return err == nil, err
}

func (c *Client) answerOptionPicker(ctx context.Context, api *chatgpt.Client, room *RoomState, target *database.Message, definition string, eventID id.EventID, command string) (string, bool, error) {
	choice, chosen := choiceIndex(command)
	if !chosen {
		return "", false, errors.New("Vote in the poll to answer this question")
	}
	current, err := api.Message(ctx, room.Profile.Room, string(target.ID))
	if err != nil {
		return "", false, err
	}
	picker, ok := current.OptionPicker()
	if !ok || current.Deleted != nil || current.Sender != room.DotMember || optionPickerDefinition(*current, picker) != definition || choice >= len(picker.Options) {
		return "", false, errors.New("This question changed; check it in ChatGPT")
	}
	key := room.Profile.Room + "\x00" + string(target.ID) + "\x00" + definition
	c.mu.RLock()
	previous, reserved := c.meta.Confirmations[key]
	c.mu.RUnlock()
	if reserved {
		if previous.Action == command && previous.Applied {
			return "ChatGPT already has this answer.", false, nil
		}
		return "", !previous.Applied, errors.New("An answer was already sent for this question; check it in ChatGPT")
	}
	outcome, err := c.optionPickerOutcome(ctx, api, room, current, picker)
	if err != nil {
		return "", false, err
	} else if outcome != "" {
		c.endClosedPoll(room, target, outcome)
		return "", false, errors.New("This question is no longer waiting for an answer; check it in ChatGPT")
	}
	if err = ctx.Err(); err != nil {
		return "", false, err
	}
	requestID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(string(c.login.ID)+"\x00"+string(eventID))).String()
	uncertain, err := c.submitDecision(ctx, key, ConfirmationAction{EventID: eventID, Action: command}, func() (bool, error) {
		sent, err := api.AnswerOptionPicker(ctx, room.Profile.Room, string(target.ID), requestID, picker.Options[choice], choice)
		return err == nil && sent.Sender == room.HumanMember, err
	})
	if err != nil {
		return "", uncertain, err
	}
	c.requestRefresh(room.Profile.Room, string(target.ID), true)
	return "ChatGPT received this answer: " + picker.Options[choice] + ".", false, nil
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

func (c *Client) refreshOpenPolls(ctx context.Context, api *chatgpt.Client, state *RoomState) error {
	rows, err := c.connector.bridge.DB.Message.GetDB().Query(ctx, `SELECT id, COALESCE(metadata->>'confirmation', ''), COALESCE(metadata->>'option_picker', '') FROM message p WHERE bridge_id=$1 AND room_id=$2 AND room_receiver=$3 AND metadata->>'poll_options' IS NOT NULL AND NOT EXISTS (SELECT 1 FROM message e WHERE e.bridge_id=p.bridge_id AND e.room_receiver=p.room_receiver AND e.id=p.id AND e.part_id=$4)`, c.connector.bridge.ID, state.Profile.Room, c.login.ID, pollEndPart)
	if err != nil {
		return err
	}
	polls := make(map[string][2]string)
	for rows.Next() {
		var id, confirmation, picker string
		if err = rows.Scan(&id, &confirmation, &picker); err != nil {
			rows.Close()
			return err
		}
		polls[id] = [2]string{confirmation, picker}
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for id, poll := range polls {
		if confirmation, picker := poll[0], poll[1]; picker != "" {
			handled, err := c.refreshOpenPicker(ctx, api, state, id, picker)
			if ctx.Err() != nil {
				return ctx.Err()
			} else if err != nil {
				c.login.Log.Debug().Err(err).Str("message_id", id).Msg("Could not check an open question poll")
			} else if handled {
				continue
			}
		} else if confirmation != "" {
			observed, err := api.ReadElicitation(ctx, state.Profile.Room, id)
			if ctx.Err() != nil {
				return ctx.Err()
			} else if err != nil {
				c.login.Log.Debug().Err(err).Str("message_id", id).Msg("Could not check an open confirmation poll")
			} else if observed.Runtime != "pending" {
				c.endClosedPoll(state, &database.Message{ID: networkid.MessageID(id)}, "Confirmation status: "+observed.Runtime)
				continue
			} else if observed.Elicitation.Definition() == confirmation && (observed.Elicitation.Status == "" || observed.Elicitation.Status == "pending") {
				continue
			}
		}
		c.refreshMu.Lock()
		if c.refresh == nil {
			c.refresh = make(map[string]map[string]bool)
		}
		if c.refresh[state.Profile.Room] == nil {
			c.refresh[state.Profile.Room] = make(map[string]bool)
		}
		c.refresh[state.Profile.Room][id] = true
		c.refreshMu.Unlock()
	}
	return nil
}

func (c *Client) optionPickerAnswered(ctx context.Context, state *RoomState, reply chatgpt.Message) (bool, error) {
	parts, err := c.connector.bridge.DB.Message.GetAllPartsByID(ctx, c.login.ID, networkid.MessageID(reply.ReplyTo.MessageID))
	if err != nil {
		return false, err
	}
	var poll *database.Message
	for _, part := range parts {
		if md, ok := part.Metadata.(*MessageMetadata); ok && md.OptionPicker != "" && part.Room == c.key(state) {
			poll = part
		}
	}
	if poll == nil {
		return false, nil
	}
	c.endClosedPoll(state, poll, "Answered: "+reply.Content.Text)
	return true, c.clearPending(ctx, reply.RequestID)
}
