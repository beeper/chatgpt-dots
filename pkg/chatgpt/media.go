package chatgpt

import (
	"bytes"
	"context"
	"errors"
	"mime"
	"mime/multipart"
	"net/textproto"
	"net/url"
	"path"
	"strings"
)

const MaxUploadSize = 20 << 20

type FileReference struct {
	Type   string `json:"type"`
	FileID string `json:"file_id"`
}

type multipartPayload struct {
	data        []byte
	contentType string
}

func (c *Client) Upload(ctx context.Context, room, filename, contentType string, data []byte) (string, error) {
	c.mu.RLock()
	verified := c.rooms[room]
	c.mu.RUnlock()
	if !verified {
		return "", errors.New("refusing unverified Dot room")
	}
	if len(data) > MaxUploadSize {
		return "", errors.New("attachment exceeds the bridge-local 20 MiB limit")
	}
	filename = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 || r == '/' || r == '\\' {
			return '_'
		}
		return r
	}, filename)
	if filename == "" {
		filename = "attachment"
	}
	if len(filename) > 255 {
		return "", errors.New("attachment filename exceeds 255 bytes")
	}
	if contentType == "" {
		contentType = mime.TypeByExtension(path.Ext(filename))
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	parsed, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return "", errors.New("invalid attachment MIME type")
	}
	var buffer bytes.Buffer
	buffer.Grow(len(data) + 1024)
	writer := multipart.NewWriter(&buffer)
	headers := textproto.MIMEHeader{}
	headers.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": "file", "filename": filename}))
	headers.Set("Content-Type", parsed)
	part, err := writer.CreatePart(headers)
	if err != nil {
		return "", errors.New("cannot prepare attachment upload")
	}
	if _, err = part.Write(data); err != nil {
		return "", errors.New("cannot prepare attachment upload")
	}
	if err = writer.Close(); err != nil {
		return "", errors.New("cannot prepare attachment upload")
	}
	var response struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	err = c.do(ctx, "POST", "/messaging/rooms/"+url.PathEscape(room)+"/files", &multipartPayload{buffer.Bytes(), writer.FormDataContentType()}, &response)
	if err != nil {
		return "", err
	}
	if response.ID == "" {
		return "", errors.New("attachment upload response has no file identity; upload outcome unknown")
	}
	return response.ID, nil
}
