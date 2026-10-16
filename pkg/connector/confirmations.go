package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type ConfirmationAction struct {
	EventID id.EventID `json:"event_id"`
	Action  string     `json:"action"`
	Applied bool       `json:"applied"`
}

func confirmationText(m chatgpt.Message, prefix string) string {
	e := m.Content.Elicitation
	if e == nil || m.Deleted != nil {
		return m.Content.Text
	}
	text := m.Content.Text
	if summary := e.Summary(); summary != "" && summary != text {
		if text != "" {
			text += "\n\n"
		}
		text += summary
	}
	if text != "" {
		text += "\n\n"
	}
	if e.Status != "" && e.Status != "pending" {
		return text + "Confirmation status: " + e.Status
	}
	if !e.Binary() {
		return text + "This request needs the ChatGPT interface: https://chatgpt.com/dots/home"
	}
	return text + fmt.Sprintf("Reply to this message with `%s accept` to accept or `%s decline` to decline. Use `%s check` to check its status. A normal chat reply does not answer this confirmation.", prefix, prefix, prefix)
}

func (c *Connector) registerConfirmationCommands() error {
	processor, ok := c.bridge.Commands.(*commands.Processor)
	if !ok {
		return errors.New("Dot confirmations require the standard bridge command processor")
	}
	for _, name := range []string{"accept", "decline", "check"} {
		processor.AddHandlers(&commands.FullHandler{Name: name, RequiresPortal: true, RequiresLogin: true,
			Help: commands.HelpMeta{Section: commands.HelpSectionMisc, Description: "Reply to a Dot confirmation to " + name + " it."}, Func: c.handleConfirmationCommand})
	}
	return nil
}

func (c *Connector) handleConfirmationCommand(ce *commands.Event) {
	message, uncertain, err := c.runConfirmationCommand(ce)
	if err != nil {
		message = err.Error()
		if uncertain {
			message = "ChatGPT has not confirmed this decision. It will not be sent again automatically. Reply to the original confirmation with " + c.bridge.Config.CommandPrefix + " check, or check it in ChatGPT."
		}
		*ce.MessageStatus = bridgev2.WrapErrorInStatus(errors.New(message)).WithMessage(message).WithIsCertain(!uncertain).WithSendNotice(false).WithStatus(event.MessageStatusFail)
	}
	ce.ReplyAdvanced(message, false, false)
}

func (c *Connector) runConfirmationCommand(ce *commands.Event) (string, bool, error) {
	if ce.Sudo || ce.User == nil || ce.Portal == nil || ce.EventID == "" || ce.ReplyTo == "" || ce.OrigRoomID != ce.RoomID || ce.RoomID != ce.Portal.MXID || strings.TrimSpace(ce.RawArgs) != "" {
		return "", false, errors.New("Reply to the original Dot confirmation in this conversation, with no extra arguments")
	}
	login, err := c.bridge.GetExistingUserLoginByID(ce.Ctx, ce.Portal.Receiver)
	if err != nil || login == nil || login.UserMXID != ce.User.MXID {
		return "", false, errors.New("This confirmation belongs to another account")
	}
	client, ok := login.Client.(*Client)
	if !ok {
		return "", false, errors.New("This is not a ChatGPT Dots conversation")
	}
	client.sendMu.Lock()
	defer client.sendMu.Unlock()
	client.mu.RLock()
	api, run := client.api, client.runContext
	room := client.meta.Rooms[string(ce.Portal.ID)]
	client.mu.RUnlock()
	if api == nil || run == nil || run.Err() != nil || !client.connected.Load() || room == nil || login.Client != client {
		return "", false, errors.New("Reconnect this Dot before answering its confirmation")
	}
	ctx, cancel := context.WithTimeout(ce.Ctx, 45*time.Second)
	defer cancel()
	stop := context.AfterFunc(run, cancel)
	defer stop()
	target, err := c.bridge.DB.Message.GetPartByMXID(ctx, ce.ReplyTo)
	if err != nil || target == nil || target.Room != client.key(room) || target.MXID != ce.ReplyTo || target.SenderID != client.dotID(room) || target.HasFakeMXID() {
		return "", false, errors.New("Reply to the original Dot confirmation in this conversation")
	}
	metadata, ok := target.Metadata.(*MessageMetadata)
	if !ok || metadata.Confirmation == "" {
		return "", false, errors.New("This message is not a synchronized confirmation; wait for its updated instructions or open ChatGPT")
	}
	verified, err := api.Verify(ctx, room.Profile)
	if err != nil {
		return "", false, err
	}
	current, err := roomState(api, room.Profile, verified)
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
	client.mu.RLock()
	previous, reserved := client.meta.Confirmations[key]
	client.mu.RUnlock()
	if ce.Command == "check" {
		if reserved && previous.Applied {
			return "ChatGPT confirmed this decision: " + previous.Action + ".", false, nil
		}
		if reserved {
			return "ChatGPT reports this confirmation as " + observed.Runtime + ". The previous decision remains unconfirmed; it will not be submitted again automatically.", false, nil
		}
		return "ChatGPT reports this confirmation as " + observed.Runtime + ".", false, nil
	}
	if ce.Command != "accept" && ce.Command != "decline" {
		return "", false, errors.New("Unknown confirmation command")
	}
	if reserved {
		if previous.Action == ce.Command && previous.Applied {
			return "ChatGPT already confirmed this decision: " + previous.Action + ".", false, nil
		}
		return "", !previous.Applied, errors.New("A decision was already submitted for this confirmation; check its status in ChatGPT")
	}
	if !observed.Elicitation.Binary() {
		return "", false, errors.New("This request needs the ChatGPT interface: https://chatgpt.com/dots/home")
	}
	if !observed.CanRespond || observed.Runtime != "pending" || observed.Elicitation.Status != "" && observed.Elicitation.Status != "pending" {
		return "", false, errors.New("This confirmation is no longer waiting for a response; check it in ChatGPT")
	}
	if err = ctx.Err(); err != nil {
		return "", false, err
	}
	// Persist before the external side effect. Ambiguous results can only be
	// inspected, never automatically re-submitted after a retry or restart.
	client.mu.Lock()
	if client.meta.Confirmations == nil {
		client.meta.Confirmations = make(map[string]ConfirmationAction)
	}
	operation := ConfirmationAction{EventID: ce.EventID, Action: ce.Command}
	client.meta.Confirmations[key] = operation
	err = login.Save(ctx)
	if err != nil {
		delete(client.meta.Confirmations, key)
	}
	client.mu.Unlock()
	if err != nil {
		return "", false, errors.New("Could not save the confirmation decision; nothing submitted")
	}
	applied, err := api.RespondElicitation(ctx, room.Profile.Room, string(target.ID), observed.Fingerprint, ce.Command)
	if err != nil || !applied {
		return "", true, errors.New("ChatGPT did not confirm this decision")
	}
	client.mu.Lock()
	operation.Applied = true
	client.meta.Confirmations[key] = operation
	err = login.Save(ctx)
	if err != nil {
		operation.Applied = false
		client.meta.Confirmations[key] = operation
	}
	client.mu.Unlock()
	if err != nil {
		return "", true, errors.New("Could not save ChatGPT's confirmation acknowledgement")
	}
	client.rescan.Store(true)
	select {
	case client.signalWake <- struct{}{}:
	default:
	}
	return "ChatGPT confirmed this decision: " + ce.Command + ".", false, nil
}
