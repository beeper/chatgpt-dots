package connector

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"html"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"github.com/rs/zerolog"
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

const maxMatrixContentBytes = 40000

const longTextCutOffNotice = "\n\n[Message cut off. View the full message in ChatGPT.]"

func matrixContentSize(content *event.MessageEventContent) int {
	raw, err := json.Marshal(content)
	if err != nil {
		return math.MaxInt
	}
	return len(raw)
}

func cutOffLongText(log zerolog.Logger, content *event.MessageEventContent, limit int) {
	original := matrixContentSize(content)
	if original <= limit {
		return
	}
	content.Format, content.FormattedBody = "", ""
	body := content.Body
	if matrixContentSize(content) > limit {
		fits := func(cut int) bool {
			for cut > 0 && !utf8.RuneStart(body[cut]) {
				cut--
			}
			content.Body = body[:cut] + longTextCutOffNotice
			return matrixContentSize(content) <= limit
		}
		cut := sort.Search(len(body), func(cut int) bool { return !fits(cut + 1) })
		fits(cut)
	}
	log.Info().
		Int("original_bytes", original).
		Int("body_bytes", len(body)).
		Int("final_bytes", matrixContentSize(content)).
		Bool("cut_off", content.Body != body).
		Msg("Shortened long ChatGPT message to fit in a Matrix event")
}
