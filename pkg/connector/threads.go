package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

type threadMessage struct {
	Room        *RoomState
	Message     chatgpt.Message
	NextRefresh time.Time
	RetryDelay  time.Duration
}

func threadBody(thread *chatgpt.ThreadStatus) string {
	if thread == nil {
		return "ChatGPT task\nLoading task details…"
	}
	title := strings.Join(strings.Fields(thread.Title), " ")
	if title == "" {
		title = "ChatGPT task"
	}
	status := map[string]string{
		"thinking": "Starting…", "inProgress": "Working…", "needsInput": "Waiting for input",
		"completed": "Completed", "failed": "Failed", "interrupted": "Interrupted",
		"unavailable": "Task details temporarily unavailable",
	}[thread.Status]
	if status == "" {
		status = "Status unavailable"
	}
	return title + "\n" + status
}

func incomingRevisions(m chatgpt.Message, existing []*database.Message) map[networkid.PartID]string {
	return incomingRevisionsWithBase(m, existing, revision(m))
}

func incomingRevisionsWithBase(m chatgpt.Message, existing []*database.Message, base string) map[networkid.PartID]string {
	result := make(map[networkid.PartID]string)
	for part := range expectedIncomingParts(m, existing) {
		result[part] = textRevision(m, base)
	}
	if m.Deleted == nil {
		for _, attachment := range mappedAttachments(m, existing) {
			if attachment.Type == "thread" {
				hash := sha256.Sum256([]byte(base + "\x00" + attachment.ThreadID + "\x00" + threadBody(m.Threads[attachment.ThreadID])))
				result[attachment.PartID] = hex.EncodeToString(hash[:])
			}
		}
	}
	return result
}

func (c *Client) prepareThreads(ctx context.Context, state *RoomState, m *chatgpt.Message) error {
	key := state.Profile.Room + "\x00" + m.ID
	refs := make(map[string]bool)
	if m.Deleted == nil {
		for _, attachment := range messageAttachments(*m) {
			if attachment.Type == "thread" && attachment.ThreadID != "" {
				refs[attachment.ThreadID] = true
			}
		}
	}
	if len(refs) == 0 {
		delete(c.threadMessages, key)
		return nil
	}
	if c.threadMessages == nil {
		c.threadMessages = make(map[string]*threadMessage)
	}
	watch := c.threadMessages[key]
	if watch == nil {
		watch = &threadMessage{Room: state}
		c.threadMessages[key] = watch
	}
	m.Threads = make(map[string]*chatgpt.ThreadStatus, len(refs))
	for threadID := range refs {
		if _, exists := watch.Message.Threads[threadID]; !exists {
			watch.NextRefresh = time.Time{}
		}
		m.Threads[threadID] = watch.Message.Threads[threadID]
	}
	existing, err := c.connector.bridge.DB.Message.GetAllPartsByID(ctx, c.login.ID, networkid.MessageID(m.ID))
	if err != nil {
		return err
	}
	for _, part := range existing {
		md, ok := part.Metadata.(*MessageMetadata)
		if ok && md.Thread != nil && refs[md.Thread.ID] && m.Threads[md.Thread.ID] == nil {
			m.Threads[md.Thread.ID] = md.Thread
		}
	}
	watch.Message = *m
	return nil
}

func (c *Client) refreshThreads(ctx context.Context, api *chatgpt.Client) error {
	var result error
	for _, watch := range c.threadMessages {
		if time.Now().Before(watch.NextRefresh) {
			continue
		}
		message := watch.Message
		message.Threads = make(map[string]*chatgpt.ThreadStatus, len(watch.Message.Threads))
		allTerminal, failed := true, false
		for threadID, previous := range watch.Message.Threads {
			message.Threads[threadID] = previous
			next, err := api.Thread(ctx, watch.Room.Profile.Room, message, threadID)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err != nil {
				c.login.Log.Warn().Err(err).Msg("Could not refresh ChatGPT task details")
				if previous == nil {
					message.Threads[threadID] = &chatgpt.ThreadStatus{ID: threadID, Status: "unavailable"}
				}
				failed = true
				continue
			}
			if previous != nil && (next.UpdatedAt < previous.UpdatedAt || next.StartedAt < previous.StartedAt || previous.Terminal() && next.TurnID == previous.TurnID && !next.Terminal() && next.UpdatedAt <= previous.UpdatedAt) {
				next = previous
			}
			message.Threads[threadID] = next
			allTerminal = allTerminal && next.Terminal()
		}
		watch.Message = message
		if err := c.deliver(ctx, watch.Room, message); err != nil {
			result = errors.Join(result, err)
			watch.NextRefresh = time.Now().Add(3 * time.Second)
			continue
		}
		delay := 3 * time.Second
		if failed {
			watch.RetryDelay = min(max(watch.RetryDelay*2, delay), time.Minute)
			delay = watch.RetryDelay
		} else {
			watch.RetryDelay = 0
			if allTerminal {
				delay = time.Minute
			}
		}
		watch.NextRefresh = time.Now().Add(delay)
	}
	return result
}

func (c *Client) saveThreadSnapshots(ctx context.Context, m chatgpt.Message, parts []*database.Message) error {
	for _, part := range parts {
		md, ok := part.Metadata.(*MessageMetadata)
		if !ok || md.Thread == nil {
			continue
		}
		latest := m.Threads[md.Thread.ID]
		if latest == nil || *latest == *md.Thread {
			continue
		}
		updated := *md
		updated.Thread = latest
		part.Metadata = &updated
		if err := c.connector.bridge.DB.Message.Update(ctx, part); err != nil {
			return err
		}
	}
	return nil
}
