package connector

import (
	"crypto/sha256"
	"encoding/hex"
	"html"
	"strings"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"github.com/yuin/goldmark"
	goldmarkhtml "github.com/yuin/goldmark/renderer/html"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/format/mdext"
)

var messageMarkdown = goldmark.New(format.Extensions, goldmark.WithExtensions(mdext.EscapeHTML), goldmark.WithRendererOptions(goldmarkhtml.WithHardWraps()))

func renderText(text string) *event.MessageEventContent {
	content := format.RenderMarkdownCustom(text, messageMarkdown)
	if content.Format == event.FormatHTML {
		content.Body = format.HTMLToText(content.FormattedBody)
	}
	return &content
}

func textRevision(m chatgpt.Message, base string) string {
	if m.Deleted != nil || len(mappedAttachments(m, nil)) != 0 || m.Content.Text == "" {
		return base
	}
	content := renderText(m.Content.Text)
	if content.Body == m.Content.Text && content.FormattedBody == "" {
		return base
	}
	hash := sha256.Sum256([]byte(base + "\x00" + content.Body + "\x00" + content.FormattedBody))
	return hex.EncodeToString(hash[:])
}

func applyCaption(content *event.MessageEventContent, text string) {
	caption := renderText(text)
	if content.MsgType == event.MsgNotice {
		if caption.Format == event.FormatHTML {
			caption.FormattedBody += "<br>" + strings.ReplaceAll(html.EscapeString(content.Body), "\n", "<br>")
		}
		caption.Body += "\n" + content.Body
	}
	content.Body = caption.Body
	content.Format = caption.Format
	content.FormattedBody = caption.FormattedBody
	content.Mentions = caption.Mentions
}
