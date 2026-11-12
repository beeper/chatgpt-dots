package chatgpt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"io"
	"net/http"
	"net/url"
	"strings"
)

func (c *Client) SelfAvatar(ctx context.Context, avatarURL string) ([]byte, error) {
	if avatarURL == "" {
		return nil, nil
	}
	u, err := url.Parse(avatarURL)
	if err != nil {
		return nil, errors.New("invalid own avatar URL")
	}
	if !selfAvatarOrigin(u) {
		return nil, fmt.Errorf("own avatar uses an unsupported storage origin %s://%s", u.Scheme, u.Host)
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, errors.New("invalid own avatar request")
	}
	req.Header.Set("User-Agent", "BeeperDotsBridge/0.1")
	client := *c.http
	var redirectErr error
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		next.Header.Del("Referer")
		if len(via) > 3 {
			redirectErr = errors.New("own avatar exceeded three redirects")
		} else if !selfAvatarOrigin(next.URL) {
			redirectErr = fmt.Errorf("own avatar redirect uses an unsupported storage origin %s://%s", next.URL.Scheme, next.URL.Host)
		}
		if redirectErr != nil {
			return http.ErrUseLastResponse
		}
		return nil
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("own avatar download failed")
	}
	defer resp.Body.Close()
	if redirectErr != nil {
		return nil, redirectErr
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("own avatar storage denied the download: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
	if err != nil || len(data) > 4<<20 {
		return nil, errors.New("own avatar exceeds the supported download size")
	}
	config, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || config.Width < 1 || config.Height < 1 || config.Width > 4096 || config.Height > 4096 {
		return nil, errors.New("own avatar must be a supported raster image no larger than 4096 pixels")
	}
	return data, nil
}

func selfAvatarOrigin(u *url.URL) bool {
	if u.Scheme != "https" || u.User != nil || u.Port() != "" {
		return false
	}
	host := u.Hostname()
	return host == "chatgpt.com" || strings.HasSuffix(host, ".oaiusercontent.com") || host == "cdn.openai.com" || strings.HasSuffix(host, ".oaistatic.com") || host == "cdn.auth0.com" || host == "gravatar.com" || strings.HasSuffix(host, ".gravatar.com") || host == "lh3.googleusercontent.com"
}
