package connector

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/crypto/attachment"
	"maunium.net/go/mautrix/event"
)

var errAttachmentTooLarge = errors.New("attachment exceeds the bridge-local 20 MiB limit")

func (c *Client) downloadAttachment(ctx context.Context, content *event.MessageEventContent) ([]byte, error) {
	var data []byte
	err := c.connector.bridge.Bot.DownloadMediaToFile(ctx, content.URL, content.File, false, func(file *os.File) error {
		if info, err := file.Stat(); err == nil && info.Size() > chatgpt.MaxUploadSize {
			return errAttachmentTooLarge
		}
		var err error
		data, err = io.ReadAll(io.LimitReader(file, chatgpt.MaxUploadSize+1))
		if err != nil {
			return err
		}
		if len(data) > chatgpt.MaxUploadSize {
			return errAttachmentTooLarge
		}
		return nil
	})
	switch {
	case err == nil:
		return data, nil
	case errors.Is(err, errAttachmentTooLarge):
		return nil, errAttachmentTooLarge
	case isAttachmentDecryptionError(err):
		return nil, errors.New("could not decrypt or verify the Matrix attachment; no message sent")
	default:
		return nil, errors.New("could not download the Matrix attachment; no message sent")
	}
}

func isAttachmentDecryptionError(err error) bool {
	return errors.Is(err, attachment.ErrHashMismatch) ||
		errors.Is(err, attachment.ErrUnsupportedVersion) ||
		errors.Is(err, attachment.ErrUnsupportedAlgorithm) ||
		errors.Is(err, attachment.ErrInvalidKey) ||
		errors.Is(err, attachment.ErrInvalidInitVector) ||
		errors.Is(err, attachment.ErrInvalidHash)
}
