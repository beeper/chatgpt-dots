package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"maunium.net/go/mautrix"
)

type respGetProxy struct {
	ProxyURL string `json:"proxy_url"`
}

func (c *Connector) getProxy(ctx context.Context, reason string) (string, error) {
	if c.Config.GetProxyURL == "" {
		return c.Config.Proxy, nil
	}
	parsed, err := url.Parse(c.Config.GetProxyURL)
	if err != nil {
		return "", errors.New("invalid proxy endpoint address")
	}
	q := parsed.Query()
	q.Set("reason", reason)
	parsed.RawQuery = q.Encode()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return "", errors.New("cannot prepare proxy request")
	}
	req.Header.Set("User-Agent", mautrix.DefaultUserAgent)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", errors.New("proxy request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("proxy request returned HTTP %d", resp.StatusCode)
	}
	var respData respGetProxy
	if err = json.NewDecoder(io.LimitReader(resp.Body, 65536)).Decode(&respData); err != nil {
		return "", errors.New("unexpected proxy response schema")
	}
	return respData.ProxyURL, nil
}

type proxyTransport struct {
	connector *Connector
	current   atomic.Pointer[http.Transport]
	failed    atomic.Bool
}

func (t *proxyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var transport http.RoundTripper = http.DefaultTransport
	if current := t.current.Load(); current != nil {
		transport = current
	}
	resp, err := transport.RoundTrip(req)
	if err != nil && req.Context().Err() == nil {
		t.failed.Store(true)
	}
	return resp, err
}

func (t *proxyTransport) update(ctx context.Context, reason string) error {
	if t.connector.Config.Proxy == "" && t.connector.Config.GetProxyURL == "" {
		return nil
	}
	addr, err := t.connector.getProxy(ctx, reason)
	if err != nil {
		return err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if addr != "" {
		parsed, err := url.Parse(addr)
		if err != nil {
			return errors.New("invalid proxy address")
		}
		transport.Proxy = http.ProxyURL(parsed)
	}
	t.failed.Store(false)
	if old := t.current.Swap(transport); old != nil {
		old.CloseIdleConnections()
	}
	return nil
}
