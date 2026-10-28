package connector

import (
	"errors"
	"testing"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

func TestWrapSendErrorPermanence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     *chatgpt.HTTPError
		certain bool
		status  event.MessageStatus
		reason  event.MessageStatusReason
		message string
	}{
		{"rejected with detail", &chatgpt.HTTPError{Status: 400, Detail: "Message is too long"}, true, event.MessageStatusFail, event.MessageStatusNetworkError, "ChatGPT rejected the request: Message is too long"},
		{"rejected without detail", &chatgpt.HTTPError{Status: 404}, true, event.MessageStatusFail, event.MessageStatusNetworkError, "ChatGPT rejected the request"},
		{"forbidden", &chatgpt.HTTPError{Status: 403, Detail: "Not allowed"}, true, event.MessageStatusRetriable, event.MessageStatusNoPermission, "ChatGPT denied the request: Not allowed"},
		{"auth expired", &chatgpt.HTTPError{Status: 401}, true, event.MessageStatusRetriable, event.MessageStatusBridgeUnavailable, "your ChatGPT session expired, reconnect ChatGPT"},
		{"cloudflare challenge", &chatgpt.HTTPError{Status: 403, Challenge: true}, true, event.MessageStatusRetriable, event.MessageStatusNetworkError, "ChatGPT's Cloudflare check blocked the request, try again later"},
		{"rate limited", &chatgpt.HTTPError{Status: 429}, true, event.MessageStatusRetriable, event.MessageStatusNetworkError, "ChatGPT is rate limiting requests, try again in a moment"},
		{"request timeout", &chatgpt.HTTPError{Status: 408}, true, event.MessageStatusRetriable, event.MessageStatusNetworkError, "ChatGPT rejected the request"},
		{"server error", &chatgpt.HTTPError{Status: 502}, true, event.MessageStatusRetriable, event.MessageStatusNetworkError, "ChatGPT had a server error, try again later"},
		{"uncertain", &chatgpt.HTTPError{Status: 400}, false, event.MessageStatusRetriable, event.MessageStatusNetworkError, "couldn't confirm ChatGPT received the message; retry this message instead of sending it again"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ms bridgev2.MessageStatus
			if !errors.As(wrapSendError(tc.err, tc.certain), &ms) {
				t.Fatal("send error is not a message status")
			}
			if ms.Status != tc.status || ms.ErrorReason != tc.reason || ms.Message != tc.message || ms.IsCertain != tc.certain {
				t.Fatalf("got status=%s reason=%s certain=%t message=%q", ms.Status, ms.ErrorReason, ms.IsCertain, ms.Message)
			}
		})
	}
}
