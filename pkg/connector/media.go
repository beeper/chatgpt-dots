package connector

import (
	"context"
	"errors"
	"io"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
)

func (c *Client) downloadAttachment(ctx context.Context, content *event.MessageEventContent) ([]byte, error) {
	intent, ok := c.connector.bridge.Bot.(*matrix.ASIntent)
	if !ok {
		return nil, errors.New("this Matrix backend does not support bounded attachment downloads")
	}
	uri := content.URL
	if content.File != nil {
		uri = content.File.URL
	}
	parsed, err := uri.Parse()
	if err != nil {
		return nil, errors.New("invalid Matrix attachment URL; no message sent")
	}
	resp, err := intent.Matrix.Download(ctx, parsed)
	if err != nil {
		return nil, errors.New("could not download the Matrix attachment; no message sent")
	}
	defer resp.Body.Close()
	if resp.ContentLength > chatgpt.MaxUploadSize {
		return nil, errors.New("attachment exceeds the bridge-local 20 MiB limit")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, chatgpt.MaxUploadSize+1))
	if len(data) > chatgpt.MaxUploadSize {
		return nil, errors.New("attachment exceeds the bridge-local 20 MiB limit")
	}
	if err != nil {
		return nil, errors.New("could not read the Matrix attachment; no message sent")
	}
	if content.File != nil {
		if err = content.File.DecryptInPlace(data); err != nil {
			return nil, errors.New("could not decrypt or verify the Matrix attachment; no message sent")
		}
	}
	return data, nil
}
