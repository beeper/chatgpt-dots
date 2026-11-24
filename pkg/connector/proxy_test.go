package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
)

func proxyConnector(cfg bridgeconfig.ProxyConfig) *Connector {
	return &Connector{bridge: &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{Proxy: cfg}}}
}

func TestProxyTransportUpdate(t *testing.T) {
	var reason string
	proxyURL := "socks5://user:pass@proxy.example:1080"
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reason = r.URL.Query().Get("reason")
		_, _ = w.Write([]byte(`{"proxy_url":"` + proxyURL + `"}`))
	}))
	defer endpoint.Close()

	proxy := &proxyTransport{connector: proxyConnector(bridgeconfig.ProxyConfig{LookupURL: endpoint.URL})}
	if err := proxy.update(context.Background(), "connect"); err != nil {
		t.Fatal(err)
	}
	if reason != "connect" {
		t.Fatalf("reason = %q, want connect", reason)
	}
	got, err := proxy.current.Load().websocket.Proxy(httptest.NewRequest(http.MethodGet, "https://chatgpt.com/", nil))
	if err != nil || got == nil || got.String() != proxyURL {
		t.Fatalf("proxy = %v (%v), want %s", got, err, proxyURL)
	}

	previous := proxyURL
	proxyURL = ""
	if err := proxy.update(context.Background(), "connect"); err == nil {
		t.Fatal("empty proxy_url should fail the update")
	}
	got, _ = proxy.current.Load().websocket.Proxy(httptest.NewRequest(http.MethodGet, "https://chatgpt.com/", nil))
	if got == nil || got.String() != previous {
		t.Fatal("failed update should keep the previous proxy")
	}
}

func TestProxyTransportMarksFailures(t *testing.T) {
	proxy := &proxyTransport{connector: proxyConnector(bridgeconfig.ProxyConfig{URL: "http://127.0.0.1:1"})}
	if err := proxy.update(context.Background(), "connect"); err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.RoundTrip(httptest.NewRequest(http.MethodGet, "https://chatgpt.com/", nil)); err == nil {
		t.Fatal("expected unreachable proxy to fail")
	}
	if !proxy.failed.Load() {
		t.Fatal("transport failure was not recorded")
	}
}

type clientHTTP struct{ http.RoundTripper }

func (clientHTTP) SetFingerprint(string) {}

func TestLoginUsesProxyOverClientHTTP(t *testing.T) {
	params := bridgev2.LoginStartParams{HTTP: clientHTTP{http.DefaultTransport}}

	direct := &Login{connector: proxyConnector(bridgeconfig.ProxyConfig{})}
	if _, err := direct.StartWithParams(context.Background(), params); err != nil {
		t.Fatal(err)
	}
	if direct.transport == nil {
		t.Fatal("login without a proxy should use the client HTTP transport")
	}

	proxied := &Login{connector: proxyConnector(bridgeconfig.ProxyConfig{LookupURL: "https://proxyserv.example"})}
	if _, err := proxied.StartWithParams(context.Background(), params); err != nil {
		t.Fatal(err)
	}
	if proxied.transport != nil {
		t.Fatal("login with a proxy should not use the client HTTP transport")
	}
}
