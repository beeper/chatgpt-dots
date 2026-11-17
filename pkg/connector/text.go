package connector

import (
	"context"
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

var matrixHTMLParser = func() *format.HTMLParser {
	parser := *format.MarkdownHTMLParser
	parser.UnderlineConverter = func(text string, _ format.Context) string { return text }
	parser.LinkConverter = func(text, href string, _ format.Context) string {
		if text == href {
			return text
		}
		return "[" + text + "](" + href + ")"
	}
	return &parser
}()

func matrixText(ctx context.Context, content *event.MessageEventContent, media bool) string {
	text := content.Body
	if media {
		text = content.GetCaption()
	}
	if text == "" || content.Format != event.FormatHTML || content.FormattedBody == "" {
		return text
	}
	if converted := matrixHTMLParser.Parse(content.FormattedBody, format.NewContext(ctx)); strings.TrimSpace(converted) != "" {
		return converted
	}
	return text
}

func textRevision(m chatgpt.Message, base string) string {
	if m.Content.Elicitation != nil {
		return base
	}
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
