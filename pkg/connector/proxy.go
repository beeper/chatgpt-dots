package connector

import (
	"context"
	"errors"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"slices"
	"sync/atomic"
	"time"

	"github.com/imroc/req/v3"
	"github.com/rs/zerolog"
)

func (c *Connector) SupportsProxy() {}

func (c *Connector) hasProxy() bool {
	return c.bridge.Config.Proxy.IsConfigured()
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

const proxyRefreshInterval = 12 * time.Hour

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

func (t *proxyTransport) host() string {
	if t == nil {
		return ""
	}
	if current := t.current.Load(); current != nil {
		return current.proxyHost
	}
	return ""
}

func (t *proxyTransport) stale() bool {
	refreshAt := t.refreshAt.Load()
	return refreshAt != 0 && time.Now().UnixNano() >= refreshAt
}

func (t *proxyTransport) postponeRefresh(d time.Duration) {
	t.refreshAt.Store(time.Now().Add(d).UnixNano())
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
	refreshAt atomic.Int64
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
	switch resp.StatusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		t.failed.Store(true)
		t.log.Warn().
			Int("status", resp.StatusCode).
			Str("proxy_host", current.proxyHost).
			Str("host", r.URL.Host).
			Str("path", r.URL.Path).
			Msg("ChatGPT gateway error")
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
	addr, err := t.connector.bridge.GetProxy(ctx, reason)
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
	if t.connector.bridge.Config.Proxy.LookupURL != "" {
		t.refreshAt.Store(time.Now().Add(proxyRefreshInterval).UnixNano())
	}
	next := newEgress(proxy)
	if old := t.current.Load(); old != nil && old.proxyHost == next.proxyHost {
		next.cookies = old.cookies
	}
	if old := t.current.Swap(next); old != nil {
		old.close()
	}
	return nil
}
