package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"slices"
	"sync/atomic"
	"time"

	"github.com/imroc/req/v3"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
)

type respGetProxy struct {
	ProxyURL string `json:"proxy_url"`
}

func (c *Connector) hasProxy() bool {
	return c.Config.Proxy != "" || c.Config.GetProxyURL != ""
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

var browserHeaders = map[string]string{
	"Accept":             "*/*",
	"Accept-Language":    "en-US,en;q=0.9",
	"Referer":            "https://chatgpt.com/",
	"Sec-Ch-Ua":          `"Not_A Brand";v="8", "Chromium";v="141", "Google Chrome";v="141"`,
	"Sec-Ch-Ua-Mobile":   "?0",
	"Sec-Ch-Ua-Platform": `"Linux"`,
	"Sec-Fetch-Dest":     "empty",
	"Sec-Fetch-Mode":     "cors",
	"Sec-Fetch-Site":     "same-origin",
}

var cloudflareCookies = []string{"__cf_bm", "__cflb", "_cfuvid", "cf_clearance"}

type egress struct {
	api       *req.Transport
	websocket *http.Transport
	cookies   *cookiejar.Jar
	proxyHost string
}

func newEgress(proxy *url.URL) *egress {
	api := req.C().ImpersonateChrome()
	websocket := http.DefaultTransport.(*http.Transport).Clone()
	var proxyHost string
	if proxy != nil {
		api.SetProxy(http.ProxyURL(proxy))
		websocket.Proxy = http.ProxyURL(proxy)
		proxyHost = proxy.Host
	}
	cookies, _ := cookiejar.New(nil)
	return &egress{api: api.GetTransport(), websocket: websocket, cookies: cookies, proxyHost: proxyHost}
}

func (e *egress) close() {
	e.api.CloseIdleConnections()
	e.websocket.CloseIdleConnections()
}

type proxyTransport struct {
	connector *Connector
	log       zerolog.Logger
	current   atomic.Pointer[egress]
	failed    atomic.Bool
}

func (t *proxyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	current := t.current.Load()
	r = r.Clone(r.Context())
	for _, cookie := range current.cookies.Cookies(r.URL) {
		r.AddCookie(cookie)
	}
	var transport http.RoundTripper = current.websocket
	if r.Header.Get("Upgrade") == "" {
		for key, value := range browserHeaders {
			if r.Header.Get(key) == "" {
				r.Header.Set(key, value)
			}
		}
		transport = current.api
	}
	resp, err := transport.RoundTrip(r)
	if err != nil {
		if r.Context().Err() != context.Canceled {
			t.failed.Store(true)
			t.log.Warn().Err(err).
				Str("proxy_host", current.proxyHost).
				Str("host", r.URL.Host).
				Str("path", r.URL.Path).
				Msg("ChatGPT transport error")
		}
		return nil, err
	}
	var keep []*http.Cookie
	for _, cookie := range resp.Cookies() {
		if slices.Contains(cloudflareCookies, cookie.Name) {
			keep = append(keep, cookie)
		}
	}
	current.cookies.SetCookies(r.URL, keep)
	return resp, nil
}

func (t *proxyTransport) update(ctx context.Context, reason string) error {
	if t.failed.Load() {
		reason = "reconnect"
	}
	addr, err := t.connector.getProxy(ctx, reason)
	if err != nil {
		return err
	}
	var proxy *url.URL
	if addr != "" {
		proxy, err = url.Parse(addr)
		if err != nil {
			return errors.New("invalid proxy address")
		}
		t.log.Info().Str("reason", reason).Str("proxy_host", proxy.Host).Msg("Using proxy")
	}
	t.failed.Store(false)
	if old := t.current.Swap(newEgress(proxy)); old != nil {
		old.close()
	}
	return nil
}
