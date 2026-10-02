package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
)

type incomingAttachment struct {
	Type         string `json:"type"`
	FileID       string `json:"file_id"`
	AttachmentID string `json:"attachment_id"`
	ThreadID     string `json:"thread_id"`
	PartID       networkid.PartID
}

func messageAttachments(m chatgpt.Message) []incomingAttachment {
	result := make([]incomingAttachment, len(m.Content.Attachments))
	occurrences := make(map[string]int)
	for i, raw := range m.Content.Attachments {
		var attachment incomingAttachment
		if json.Unmarshal(raw, &attachment) != nil {
			attachment = incomingAttachment{Type: "unsupported"}
		}
		key := "file:" + attachment.FileID
		if attachment.AttachmentID != "" {
			key = "attachment:" + attachment.AttachmentID
		} else if attachment.Type != "file" {
			key = "unsupported:" + attachment.Type
		}
		occurrences[key]++
		hash := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", key, occurrences[key])))
		attachment.PartID = networkid.PartID("attachment-" + hex.EncodeToString(hash[:]))
		result[i] = attachment
	}
	return result
}

func outgoingMediaAnchor(existing []*database.Message) bool {
	for _, part := range existing {
		if md, ok := part.Metadata.(*MessageMetadata); ok && part.PartID == "" && md.OutgoingMedia {
			return true
		}
	}
	return false
}

func mappedAttachments(m chatgpt.Message, existing []*database.Message) []incomingAttachment {
	var attachments []incomingAttachment
	for _, attachment := range messageAttachments(m) {
		if attachment.Type != "link" {
			attachments = append(attachments, attachment)
		}
	}
	if len(attachments) > 0 && outgoingMediaAnchor(existing) {
		attachments[0].PartID = ""
	}
	return attachments
}

func textPartID(m chatgpt.Message, existing []*database.Message) networkid.PartID {
	for _, part := range existing {
		if md, ok := part.Metadata.(*MessageMetadata); part.PartID == "" || ok && md.TextAnchor {
			return part.PartID
		}
	}
	for _, attachment := range messageAttachments(m) {
		if attachment.Type == "link" {
			for _, part := range existing {
				if part.PartID == attachment.PartID {
					return part.PartID
				}
			}
		}
	}
	return ""
}

func expectedIncomingParts(m chatgpt.Message, existing []*database.Message) map[networkid.PartID]bool {
	parts := make(map[networkid.PartID]bool)
	attachments := mappedAttachments(m, existing)
	if m.Deleted != nil {
		parts["deleted"] = true
	} else if len(attachments) == 0 {
		if m.Content.Text != "" {
			parts[textPartID(m, existing)] = true
		}
	} else {
		for _, attachment := range attachments {
			parts[attachment.PartID] = true
		}
	}
	return parts
}

func incomingSaved(m chatgpt.Message, existing []*database.Message) bool {
	expected := incomingRevisions(m, existing)
	if len(existing) != len(expected) {
		return false
	}
	for _, part := range existing {
		md, ok := part.Metadata.(*MessageMetadata)
		if !ok || md.Revision != expected[part.PartID] {
			return false
		}
		delete(expected, part.PartID)
	}
	return len(expected) == 0
}

func (c *Client) convertIncoming(ctx context.Context, state *RoomState, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, m chatgpt.Message, existing []*database.Message) (*bridgev2.ConvertedMessage, error) {
	revisions := incomingRevisions(m, existing)
	current := make(map[networkid.PartID]bool)
	for _, old := range existing {
		if md, ok := old.Metadata.(*MessageMetadata); ok && md.Revision == revisions[old.PartID] {
			current[old.PartID] = true
		}
	}
	result := &bridgev2.ConvertedMessage{}
	attachments := mappedAttachments(m, existing)
	if m.Deleted == nil && m.Content.Text == "" && len(attachments) == 0 {
		return result, nil
	}
	add := func(id networkid.PartID, content *event.MessageEventContent, thread *chatgpt.ThreadStatus) {
		result.Parts = append(result.Parts, &bridgev2.ConvertedMessagePart{ID: id, Type: event.EventMessage, Content: content, DBMetadata: &MessageMetadata{Revision: revisions[id], Thread: thread, TextAnchor: len(attachments) == 0 && m.Deleted == nil, OutgoingMedia: id == "" && outgoingMediaAnchor(existing) && m.Deleted == nil}})
	}
	if m.Deleted != nil || len(attachments) == 0 {
		partID := textPartID(m, existing)
		if m.Deleted != nil {
			partID = "deleted"
		}
		if !current[partID] {
			content := renderText(m.Content.Text)
			if m.Deleted != nil {
				content = &event.MessageEventContent{MsgType: event.MsgNotice, Body: "[Message deleted in ChatGPT]"}
			}
			add(partID, content, nil)
		}
		return result, nil
	}
	c.mu.RLock()
	api := c.api
	c.mu.RUnlock()
	if api == nil || ctx.Err() != nil {
		return nil, errors.New("ChatGPT connection unavailable for attachment download")
	}
	for i, attachment := range attachments {
		if current[attachment.PartID] {
			continue
		}
		content := &event.MessageEventContent{MsgType: event.MsgNotice, Body: "[Unsupported ChatGPT attachment]"}
		var thread *chatgpt.ThreadStatus
		if attachment.Type == "file" {
			var err error
			content, err = incomingFile(ctx, api, state, portal, intent, attachment.FileID)
			if err != nil {
				var unavailable *chatgpt.AttachmentUnavailableError
				if !errors.As(err, &unavailable) || ctx.Err() != nil {
					return nil, err
				}
				content = &event.MessageEventContent{MsgType: event.MsgNotice, Body: "[ChatGPT attachment unavailable: " + unavailable.Reason + "]"}
			}
		} else if attachment.Type == "thread" {
			thread = m.Threads[attachment.ThreadID]
			content = &event.MessageEventContent{MsgType: event.MsgNotice, Body: threadBody(thread)}
		}
		if i == 0 && m.Content.Text != "" {
			applyCaption(content, m.Content.Text)
		}
		add(attachment.PartID, content, thread)
	}
	return result, nil
}

func incomingFile(ctx context.Context, api *chatgpt.Client, state *RoomState, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, fileID string) (*event.MessageEventContent, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if fileID == "" {
		return nil, &chatgpt.AttachmentUnavailableError{Reason: "missing file identity"}
	}
	file, err := api.DownloadAttachment(ctx, state.Profile.Room, fileID)
	if err != nil {
		return nil, err
	}
	if file == nil || len(file.Data) > chatgpt.MaxDownloadSize {
		return nil, errors.New("ChatGPT attachment violates the download contract; checkpoint retained")
	}
	name := path.Base(strings.ReplaceAll(file.Name, "\\", "/"))
	if name == "" || name == "." || name == "/" {
		name = "attachment"
	}
	mimeType, _, err := mime.ParseMediaType(file.MIME)
	if err != nil || mimeType == "" {
		mimeType = "application/octet-stream"
	}
	content := &event.MessageEventContent{MsgType: event.MsgFile, Body: name, FileName: name, Info: &event.FileInfo{MimeType: mimeType, Size: len(file.Data), Width: max(0, file.Width), Height: max(0, file.Height)}}
	if strings.HasPrefix(mimeType, "image/") {
		content.MsgType = event.MsgImage
	} else if strings.HasPrefix(mimeType, "video/") {
		content.MsgType = event.MsgVideo
	}
	content.URL, content.File, err = intent.UploadMedia(ctx, portal.MXID, file.Data, name, mimeType)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	var httpErr mautrix.HTTPError
	if errors.Is(err, mautrix.MTooLarge) || (errors.As(err, &httpErr) && httpErr.IsStatus(http.StatusRequestEntityTooLarge)) {
		return nil, &chatgpt.AttachmentUnavailableError{Reason: "file exceeds the Matrix media limit"}
	} else if err != nil {
		return nil, errors.New("Matrix attachment upload failed; checkpoint retained")
	}
	return content, nil
}
