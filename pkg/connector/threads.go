package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
)

const taskWatchExpiry = time.Hour

type threadMessage struct {
	Room          *RoomState
	Message       chatgpt.Message
	NextRefresh   time.Time
	Backoff       time.Duration
	TaskSynced    map[string]string
	TaskBridged   map[networkid.MessageID]bool
	TaskTurnsDone map[string]bool
	TaskFailures  map[string]int
}

const (
	taskItemPrefix   = "task:"
	taskItemAttempts = 5
)

func taskItemID(threadID, itemID string) networkid.MessageID {
	return networkid.MessageID(taskItemPrefix + threadID + ":" + itemID)
}

func isTaskItem(id networkid.MessageID) bool {
	return strings.HasPrefix(string(id), taskItemPrefix)
}

func taskSyncKey(thread *chatgpt.ThreadStatus) string {
	return thread.TurnID + "\x00" + strconv.FormatBool(thread.Terminal())
}

func (c *Client) bridgeTaskItems(ctx context.Context, api *chatgpt.Client, watch *threadMessage, request chatgpt.Message, threadID string) error {
	turns, err := api.TaskTurns(ctx, watch.Room.Profile.Room, request, threadID, func(turnID string) bool { return watch.TaskTurnsDone[turnID] })
	if err != nil {
		return err
	}
	root := networkid.MessageID(watch.Message.ID)
	if watch.Message.ReplyRootMessageID != "" {
		root = networkid.MessageID(watch.Message.ReplyRootMessageID)
	}
	for _, turn := range turns {
		for _, item := range turn.Items {
			id := taskItemID(threadID, item.ID)
			if watch.TaskBridged[id] {
				continue
			}
			sender := bridgev2.EventSender{Sender: c.dotID(watch.Room)}
			if item.FromUser {
				sender = bridgev2.EventSender{Sender: c.GetUserID(), SenderLogin: c.login.ID, IsFromMe: true}
			}
			timestamp := item.Timestamp
			if timestamp.IsZero() {
				timestamp = time.Now()
			}
			content := renderText(item.Text)
			cutOffLongText(c.login.Log, content, maxMatrixContentBytes)
			result := c.login.QueueRemoteEvent(&simplevent.PreConvertedMessage{
				EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventMessage, PortalKey: c.key(watch.Room), Sender: sender, Timestamp: timestamp},
				ID:        id,
				Data:      &bridgev2.ConvertedMessage{ThreadRoot: &root, Parts: []*bridgev2.ConvertedMessagePart{{Type: event.EventMessage, Content: content, DBMetadata: &MessageMetadata{}}}},
			})
			if !result.Success {
				return errors.New("bridge could not queue ChatGPT task reply")
			}
			if watch.TaskBridged == nil {
				watch.TaskBridged = make(map[networkid.MessageID]bool)
			}
			watch.TaskBridged[id] = true
		}
		if turn.Finished {
			if watch.TaskTurnsDone == nil {
				watch.TaskTurnsDone = make(map[string]bool)
			}
			watch.TaskTurnsDone[turn.ID] = true
		}
	}
	return nil
}

func threadsSettled(threads map[string]*chatgpt.ThreadStatus) bool {
	for _, thread := range threads {
		if !thread.Terminal() || time.Since(time.Unix(thread.UpdatedAt, 0)) < taskWatchExpiry {
			return false
		}
	}
	return len(threads) > 0
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
	result := incomingRevisionsWithBase(m, existing, revision(m))
	attachments := mappedAttachments(m, existing)
	if m.Deleted != nil {
		return result
	}
	expected := expectedIncomingParts(m, existing)
	for _, old := range existing {
		if md, ok := old.Metadata.(*MessageMetadata); ok && expected[old.PartID] {
			result[old.PartID] = md.Revision
		}
	}
	if len(attachments) == 0 {
		if m.Content.Text != "" || m.Content.Elicitation != nil {
			result[textPartID(m, existing)] = textRevision(m, revision(m))
		}
		return result
	}
	caption := captionPart(m, attachments, existing)
	for _, old := range existing {
		if md, ok := old.Metadata.(*MessageMetadata); ok && md.TextAnchor && old.PartID == caption {
			part := m
			part.Content.Attachments = nil
			result[caption] = textRevision(part, revision(part))
		}
	}
	for _, attachment := range attachments {
		part := m
		raw, _ := json.Marshal(attachment)
		part.Content.Attachments = []json.RawMessage{raw}
		if attachment.PartID != caption {
			part.Content.Text = ""
			part.Content.Elicitation = nil
		}
		base := revision(part)
		if attachment.Type == "thread" {
			hash := sha256.Sum256([]byte(base + "\x00" + attachment.ThreadID + "\x00" + threadBody(m.Threads[attachment.ThreadID])))
			base = hex.EncodeToString(hash[:])
		}
		result[attachment.PartID] = base
	}
	return result
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

func (c *Client) prepareThreads(state *RoomState, m *chatgpt.Message, existing []*database.Message) {
	key := state.Profile.Room + "\x00" + m.ID
	refs := make(map[string]bool)
	if m.Deleted == nil {
		for _, attachment := range mappedAttachments(*m, existing) {
			if attachment.Type == "thread" && attachment.ThreadID != "" {
				refs[attachment.ThreadID] = true
			}
		}
	}
	if len(refs) == 0 {
		delete(c.threadMessages, key)
		return
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
	for _, part := range existing {
		md, ok := part.Metadata.(*MessageMetadata)
		if ok && md.Thread != nil && refs[md.Thread.ID] && m.Threads[md.Thread.ID] == nil {
			m.Threads[md.Thread.ID] = md.Thread
		}
	}
	watch.Message = *m
}

func (c *Client) refreshThreads(ctx context.Context, api *chatgpt.Client) error {
	var result error
	for key, watch := range c.threadMessages {
		if _, pending := watch.Room.Media[watch.Message.ID]; pending {
			continue
		}
		if time.Now().Before(watch.NextRefresh) {
			continue
		}
		message := watch.Message
		message.Threads = make(map[string]*chatgpt.ThreadStatus, len(watch.Message.Threads))
		allTerminal, failed, changed := true, false, false
		for threadID, previous := range watch.Message.Threads {
			message.Threads[threadID] = previous
			request := message
			request.Content.Attachments = slices.Clone(message.Content.Attachments)
			raw, _ := json.Marshal(incomingAttachment{Type: "thread", ThreadID: threadID})
			request.Content.Attachments = append(request.Content.Attachments, raw)
			next, err := api.Thread(ctx, watch.Room.Profile.Room, request, threadID)
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
			changed = changed || previous == nil || *next != *previous
			message.Threads[threadID] = next
			if key := taskSyncKey(next); watch.TaskSynced[threadID] != key {
				err = c.bridgeTaskItems(ctx, api, watch, request, threadID)
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err != nil {
					if watch.TaskFailures == nil {
						watch.TaskFailures = make(map[string]int)
					}
					watch.TaskFailures[threadID]++
					log := c.login.Log.Warn().Err(err).Str("task_id", threadID).Int("attempt", watch.TaskFailures[threadID])
					var httpErr *chatgpt.HTTPError
					var schemaErr *chatgpt.SchemaError
					if errors.As(err, &httpErr) {
						log = log.Str("path", httpErr.Path).Str("detail", httpErr.Body)
					} else if errors.As(err, &schemaErr) {
						log = log.Str("path", schemaErr.Path).
							Int64("content_length", schemaErr.ContentLength).
							Int64("bytes_read", schemaErr.BytesRead).
							Bool("truncated", schemaErr.Truncated).
							Str("decode_error", schemaErr.Cause).
							Str("decode_field", schemaErr.Field).
							Int64("decode_offset", schemaErr.Offset)
					}
					log.Msg("Could not bridge ChatGPT task replies")
				}
				if err == nil || watch.TaskFailures[threadID] >= taskItemAttempts {
					if watch.TaskSynced == nil {
						watch.TaskSynced = make(map[string]string)
					}
					watch.TaskSynced[threadID] = key
					delete(watch.TaskFailures, threadID)
				} else {
					failed = true
				}
			}
			allTerminal = allTerminal && next.Terminal()
		}
		watch.Message = message
		if err := c.deliverMedia(ctx, watch.Room, message, watch.Room.Baseline); err != nil {
			result = errors.Join(result, err)
			watch.NextRefresh = time.Now().Add(3 * time.Second)
			continue
		}
		if !failed && threadsSettled(watch.Message.Threads) {
			delete(c.threadMessages, key)
			continue
		}
		switch {
		case failed:
			watch.Backoff = min(max(watch.Backoff*2, 3*time.Second), time.Minute)
		case changed:
			watch.Backoff = 3 * time.Second
		case allTerminal:
			watch.Backoff = time.Minute
		default:
			watch.Backoff = min(max(watch.Backoff*2, 3*time.Second), 30*time.Second)
		}
		watch.NextRefresh = time.Now().Add(watch.Backoff)
	}
	return result
}

func (c *Client) saveMessageMetadata(ctx context.Context, m chatgpt.Message, parts []*database.Message) error {
	expected := expectedIncomingParts(m, parts)
	attachments := mappedAttachments(m, parts)
	for _, part := range parts {
		md, ok := part.Metadata.(*MessageMetadata)
		if !ok || !expected[part.PartID] {
			continue
		}
		updated := *md
		if m.Deleted == nil && md.Attachment == nil {
			for _, attachment := range attachments {
				if attachment.PartID == part.PartID {
					updated.Attachment = &attachment
					updated.Caption = attachment.PartID == captionPart(m, attachments, parts)
					break
				}
			}
		}
		if m.Updated.After(updated.Updated) {
			updated.Updated = m.Updated
		}
		if md.Thread != nil {
			if latest := m.Threads[md.Thread.ID]; latest != nil {
				updated.Thread = latest
			}
		}
		if updated.Attachment == md.Attachment && updated.Updated.Equal(md.Updated) && (updated.Thread == md.Thread || updated.Thread != nil && md.Thread != nil && *updated.Thread == *md.Thread) {
			continue
		}
		part.Metadata = &updated
		if err := c.connector.bridge.DB.Message.Update(ctx, part); err != nil {
			return err
		}
	}
	return nil
}
