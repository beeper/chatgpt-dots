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
	Type         string           `json:"type"`
	FileID       string           `json:"file_id"`
	AttachmentID string           `json:"attachment_id"`
	ThreadID     string           `json:"thread_id"`
	PartID       networkid.PartID `json:"-"`
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
	seen := make(map[networkid.PartID]bool)
	for _, attachment := range messageAttachments(m) {
		if attachment.Type != "link" {
			attachments = append(attachments, attachment)
			seen[attachment.PartID] = true
		}
	}
	for _, part := range existing {
		md, ok := part.Metadata.(*MessageMetadata)
		if !ok || !md.OutgoingMedia || part.PartID != "" {
			continue
		}
		for i, attachment := range attachments {
			if md.Attachment == nil || attachment.Type == md.Attachment.Type && (attachment.AttachmentID != "" && attachment.AttachmentID == md.Attachment.AttachmentID || attachment.AttachmentID == "" && attachment.FileID == md.Attachment.FileID) {
				attachments[i].PartID = ""
				seen[""] = true
				break
			}
		}
	}
	if m.Deleted == nil {
		for _, part := range existing {
			md, ok := part.Metadata.(*MessageMetadata)
			if !ok || seen[part.PartID] {
				continue
			}
			var attachment incomingAttachment
			if md.Attachment != nil {
				attachment = *md.Attachment
			} else if md.Thread != nil {
				attachment = incomingAttachment{Type: "thread", ThreadID: md.Thread.ID}
			} else {
				continue
			}
			attachment.PartID = part.PartID
			attachments = append(attachments, attachment)
			seen[part.PartID] = true
		}
	}
	return attachments
}

func captionPart(m chatgpt.Message, attachments []incomingAttachment, existing []*database.Message) networkid.PartID {
	for _, part := range existing {
		if md, ok := part.Metadata.(*MessageMetadata); ok && md.TextAnchor && (m.Content.Text != "" || m.Content.Elicitation != nil) {
			return part.PartID
		}
	}
	for _, part := range existing {
		if md, ok := part.Metadata.(*MessageMetadata); ok && md.Caption {
			for _, attachment := range attachments {
				if attachment.PartID == part.PartID {
					return part.PartID
				}
			}
		}
	}
	if len(attachments) > 0 {
		return attachments[0].PartID
	}
	return ""
}

func textPartID(m chatgpt.Message, existing []*database.Message) networkid.PartID {
	for _, part := range existing {
		if md, ok := part.Metadata.(*MessageMetadata); ok && md.TextAnchor || part.PartID == "" && (!ok || !md.OutgoingMedia) {
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
	if outgoingMediaAnchor(existing) {
		return "caption"
	}
	return ""
}

func expectedIncomingParts(m chatgpt.Message, existing []*database.Message) map[networkid.PartID]bool {
	parts := make(map[networkid.PartID]bool)
	attachments := mappedAttachments(m, existing)
	if m.Deleted != nil {
		parts["deleted"] = true
		return parts
	}
	if len(attachments) == 0 {
		if m.Content.Text != "" || m.Content.Elicitation != nil {
			parts[textPartID(m, existing)] = true
		}
	} else {
		for _, attachment := range attachments {
			parts[attachment.PartID] = true
		}
		parts[captionPart(m, attachments, existing)] = true
	}
	links := make(map[networkid.PartID]bool)
	for _, attachment := range messageAttachments(m) {
		if attachment.Type == "link" {
			links[attachment.PartID] = true
		}
	}
	for _, part := range existing {
		if md, ok := part.Metadata.(*MessageMetadata); ok && (md.OutgoingMedia || !md.TextAnchor && strings.HasPrefix(string(part.PartID), "attachment-") && !links[part.PartID]) {
			parts[part.PartID] = true
		}
	}
	return parts
}

func incomingStale(m chatgpt.Message, existing []*database.Message) bool {
	for _, part := range existing {
		if part.PartID == "deleted" && m.Deleted == nil {
			return true
		}
		if md, ok := part.Metadata.(*MessageMetadata); ok && !m.Updated.IsZero() && md.Updated.After(m.Updated) {
			return true
		}
	}
	return false
}

func incomingSaved(m chatgpt.Message, existing []*database.Message) bool {
	expected := incomingRevisions(m, existing)
	for _, part := range existing {
		revision, wanted := expected[part.PartID]
		if !wanted {
			continue
		}
		md, ok := part.Metadata.(*MessageMetadata)
		if !ok || md.Revision != revision {
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
	caption := captionPart(m, attachments, existing)
	if m.Deleted == nil && m.Content.Text == "" && m.Content.Elicitation == nil && len(attachments) == 0 {
		return result, nil
	}
	add := func(id networkid.PartID, content *event.MessageEventContent, thread *chatgpt.ThreadStatus, attachment *incomingAttachment, media *event.MessageEventContent) {
		confirmation := ""
		if m.Deleted == nil && m.Sender == state.DotMember {
			confirmation = m.Content.Elicitation.Definition()
		}
		result.Parts = append(result.Parts, &bridgev2.ConvertedMessagePart{ID: id, Type: event.EventMessage, Content: content, DBMetadata: &MessageMetadata{Attachment: attachment, Media: media, Caption: attachment != nil && id == caption, Updated: m.Updated, Confirmation: confirmation, Revision: revisions[id], Thread: thread, TextAnchor: attachment == nil && m.Deleted == nil, OutgoingMedia: id == "" && outgoingMediaAnchor(existing) && m.Deleted == nil}})
	}
	text := confirmationText(m, c.connector.bridge.Config.CommandPrefix)
	if m.Deleted != nil || len(attachments) == 0 {
		partID := textPartID(m, existing)
		if m.Deleted != nil {
			partID = "deleted"
		}
		if !current[partID] {
			content := renderText(text)
			if m.Deleted != nil {
				content = &event.MessageEventContent{MsgType: event.MsgNotice, Body: "[Message deleted in ChatGPT]"}
			}
			add(partID, content, nil, nil, nil)
		}
		return result, nil
	}
	if !current[caption] {
		for _, old := range existing {
			if md, ok := old.Metadata.(*MessageMetadata); ok && md.TextAnchor && old.PartID == caption {
				add(caption, renderText(text), nil, nil, nil)
				break
			}
		}
	}
	c.mu.RLock()
	api := c.api
	c.mu.RUnlock()
	if api == nil || ctx.Err() != nil {
		return nil, errors.New("ChatGPT connection unavailable for attachment download")
	}
	for _, attachment := range attachments {
		if current[attachment.PartID] {
			continue
		}
		content := &event.MessageEventContent{MsgType: event.MsgNotice, Body: "[Unsupported ChatGPT attachment]"}
		var thread *chatgpt.ThreadStatus
		var media *event.MessageEventContent
		if attachment.Type == "file" {
			var err error
			for _, old := range existing {
				if md, ok := old.Metadata.(*MessageMetadata); ok && old.PartID == attachment.PartID && md.Attachment != nil && md.Attachment.FileID == attachment.FileID && md.Media != nil {
					cached := *md.Media
					content = &cached
					media = md.Media
					break
				}
			}
			if media == nil {
				content, err = incomingFile(ctx, api, state, portal, intent, attachment.FileID)
			}
			if err != nil {
				var unavailable *chatgpt.AttachmentUnavailableError
				if !errors.As(err, &unavailable) || ctx.Err() != nil {
					return nil, err
				}
				content = &event.MessageEventContent{MsgType: event.MsgNotice, Body: "[ChatGPT attachment unavailable: " + unavailable.Reason + "]"}
			}
			if err == nil && media == nil {
				cached := *content
				media = &cached
			}
		} else if attachment.Type == "thread" {
			thread = m.Threads[attachment.ThreadID]
			content = &event.MessageEventContent{MsgType: event.MsgNotice, Body: threadBody(thread)}
		}
		if attachment.PartID == caption && text != "" {
			applyCaption(content, text)
		}
		add(attachment.PartID, content, thread, &attachment, media)
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
