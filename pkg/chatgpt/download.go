package chatgpt

import (
	"bytes"
	"context"
	"errors"
	"image"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"
	"unicode/utf8"
)

const MaxDownloadSize = 20 << 20

type AttachmentUnavailableError struct{ Reason string }

func (e *AttachmentUnavailableError) Error() string { return e.Reason }

type FileData struct {
	Name   string
	MIME   string
	Data   []byte
	Width  int
	Height int
}

func (c *Client) DownloadAttachment(ctx context.Context, room, fileID string) (*FileData, error) {
	c.mu.RLock()
	verified := c.rooms[room]
	c.mu.RUnlock()
	if !verified || fileID == "" {
		return nil, errors.New("refusing attachment from unverified Dot room or missing file identity")
	}
	var metadata struct {
		ID     string `json:"id"`
		FileID string `json:"file_id"`
		URL    string `json:"download_url"`
		Name   string `json:"name"`
		MIME   string `json:"mime_type"`
	}
	if err := c.do(ctx, "GET", "/messaging/rooms/"+url.PathEscape(room)+"/files/"+url.PathEscape(fileID), nil, &metadata); err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && (httpErr.Status == http.StatusNotFound || httpErr.Status == http.StatusGone) {
			return nil, &AttachmentUnavailableError{Reason: "ChatGPT no longer provides this file"}
		}
		return nil, err
	}
	if (metadata.ID != "" && metadata.ID != fileID) || (metadata.FileID != "" && metadata.FileID != fileID) {
		return nil, errors.New("attachment metadata identity mismatch")
	}
	if metadata.URL == "" {
		return nil, errors.New("ChatGPT attachment download is not ready")
	}
	u, err := url.Parse(metadata.URL)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Port() != "" || u.Fragment != "" || !strings.HasSuffix(strings.ToLower(u.Hostname()), ".oaiusercontent.com") {
		return nil, &AttachmentUnavailableError{Reason: "unsupported attachment storage origin"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, errors.New("invalid attachment download request")
	}
	req.Header.Set("User-Agent", "BeeperDotsBridge/0.1")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, errors.New("attachment storage download failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		return nil, &AttachmentUnavailableError{Reason: "ChatGPT attachment is no longer available"}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("attachment storage denied the download")
	}
	if resp.ContentLength > MaxDownloadSize {
		return nil, &AttachmentUnavailableError{Reason: "file exceeds the bridge-local 20 MiB limit"}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxDownloadSize+1))
	if err != nil {
		return nil, errors.New("attachment download interrupted")
	}
	if len(data) > MaxDownloadSize {
		return nil, &AttachmentUnavailableError{Reason: "file exceeds the bridge-local 20 MiB limit"}
	}
	name := strings.Map(func(r rune) rune {
		if r < 32 || r == 127 || r == '/' || r == '\\' {
			return '_'
		}
		return r
	}, metadata.Name)
	if name == "" || name == "." || name == ".." {
		name = "attachment"
	}
	if len(name) > 255 {
		name = name[:255]
		for !utf8.ValidString(name) {
			name = name[:len(name)-1]
		}
	}
	mimeType, _, err := mime.ParseMediaType(metadata.MIME)
	if err != nil || mimeType == "" || mimeType == "application/octet-stream" {
		mimeType, _, _ = mime.ParseMediaType(http.DetectContentType(data))
		if mimeType == "application/octet-stream" {
			if extensionType := mime.TypeByExtension(strings.ToLower(path.Ext(name))); extensionType != "" {
				mimeType, _, _ = mime.ParseMediaType(extensionType)
			}
		}
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	file := &FileData{Name: name, MIME: mimeType, Data: data}
	if config, _, err := image.DecodeConfig(bytes.NewReader(data)); err == nil && config.Width > 0 && config.Height > 0 {
		file.Width, file.Height = config.Width, config.Height
	}
	return file, nil
}
