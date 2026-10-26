package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
)

const (
	stateAuthExpired             status.BridgeStateErrorCode = "chatgpt-auth-expired"
	stateNoCredentials           status.BridgeStateErrorCode = "chatgpt-no-credentials"
	stateBadCredentials          status.BridgeStateErrorCode = "chatgpt-bad-credentials"
	stateIdentityChanged         status.BridgeStateErrorCode = "chatgpt-identity-changed"
	stateCloudflareChallenge     status.BridgeStateErrorCode = "chatgpt-cloudflare-challenge"
	stateForbidden               status.BridgeStateErrorCode = "chatgpt-forbidden"
	stateRateLimited             status.BridgeStateErrorCode = "chatgpt-rate-limited"
	stateServerError             status.BridgeStateErrorCode = "chatgpt-server-error"
	stateRequestFailed           status.BridgeStateErrorCode = "chatgpt-request-failed"
	stateNetworkError            status.BridgeStateErrorCode = "chatgpt-network-error"
	stateSyncFailed              status.BridgeStateErrorCode = "chatgpt-sync-failed"
	stateCredentialCleanupFailed status.BridgeStateErrorCode = "chatgpt-credential-cleanup-failed"
	stateRestoreFailed           status.BridgeStateErrorCode = "chatgpt-restore-failed"
	stateDotUnavailable          status.BridgeStateErrorCode = "chatgpt-dot-unavailable"
)

func init() {
	status.BridgeStateHumanErrors.Update(status.BridgeStateErrorMap{
		stateAuthExpired:             "Your ChatGPT session expired, sign in again",
		stateNoCredentials:           "Your ChatGPT session isn't stored on this bridge, sign in again",
		stateBadCredentials:          "Your saved ChatGPT session is invalid, sign in again",
		stateIdentityChanged:         "This login now belongs to a different ChatGPT account, sign in with the original account",
		stateCloudflareChallenge:     "ChatGPT's Cloudflare check blocked the bridge, retrying",
		stateForbidden:               "ChatGPT denied a request, retrying",
		stateRateLimited:             "ChatGPT is rate limiting the bridge, retrying",
		stateServerError:             "ChatGPT is having server problems, retrying",
		stateRequestFailed:           "A ChatGPT request failed, retrying",
		stateNetworkError:            "Can't reach ChatGPT, retrying",
		stateSyncFailed:              "Couldn't sync with ChatGPT, retrying",
		stateCredentialCleanupFailed: "Couldn't remove your ChatGPT session from the bridge",
		stateRestoreFailed:           "Couldn't restore the previous ChatGPT login, sign in with the original account",
		stateDotUnavailable:          "Your linked Dot is no longer in ChatGPT, sign in again to connect your current Dot",
	})
}

func badCredentialsState(code status.BridgeStateErrorCode, err error) status.BridgeState {
	return status.BridgeState{
		StateEvent: status.StateBadCredentials,
		Error:      code,
		UserAction: status.UserActionRelogin,
		Info:       map[string]any{"go_error": err.Error()},
	}
}

func isTransportError(err error) bool {
	return errors.Is(err, chatgpt.ErrTransport) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func wrapSendError(err error, certain bool) error {
	if err == nil || errors.As(err, &bridgev2.MessageStatus{}) {
		return err
	}
	ms := bridgev2.WrapErrorInStatus(err).WithIsCertain(certain).WithSendNotice(true)
	var httpErr *chatgpt.HTTPError
	switch {
	case errors.As(err, &httpErr):
		switch {
		case httpErr.Status == http.StatusUnauthorized:
			ms = ms.WithErrorReason(event.MessageStatusNoPermission).WithMessage("your ChatGPT session expired, reconnect ChatGPT")
		case httpErr.Challenge:
			ms = ms.WithErrorReason(event.MessageStatusNetworkError).WithMessage("ChatGPT's Cloudflare check blocked the request, try again later")
		case httpErr.Status == http.StatusTooManyRequests:
			ms = ms.WithErrorReason(event.MessageStatusNetworkError).WithMessage("ChatGPT is rate limiting requests, try again in a moment")
		case httpErr.Status >= 500:
			ms = ms.WithErrorReason(event.MessageStatusNetworkError).WithMessage("ChatGPT had a server error, try again later")
		default:
			ms = ms.WithErrorReason(event.MessageStatusNetworkError).WithMessage("ChatGPT rejected the request")
		}
	case isTransportError(err):
		ms = ms.WithMessage("couldn't reach ChatGPT, try again")
	default:
		ms = ms.WithMessage("failed to send the message to ChatGPT")
	}
	if !certain {
		ms = ms.WithMessage("couldn't confirm ChatGPT received the message; retry this message instead of sending it again")
	}
	return ms
}

func relinkStatus(err error, certain bool) error {
	return bridgev2.WrapErrorInStatus(err).
		WithMessage("this Dot chat needs to be re-linked, reconnect ChatGPT").
		WithErrorReason(event.MessageStatusNoPermission).
		WithStatus(event.MessageStatusFail).
		WithIsCertain(certain).
		WithSendNotice(true)
}

var errDotUnavailable = errors.New("the linked Dot is no longer the account's Dot in ChatGPT")

var errNoBaseline = errors.New("no baseline message ID in the Dot room; send a message in ChatGPT before linking")

func wrapLoginError(err error) error {
	if err == nil || errors.As(err, &bridgev2.RespError{}) {
		return err
	}
	var mapped bridgev2.RespError
	var httpErr *chatgpt.HTTPError
	errors.As(err, &httpErr)
	switch {
	case httpErr != nil && httpErr.PrimaryUnavailable:
		mapped = bridgev2.RespError(mautrix.MForbidden.WithMessage("This ChatGPT account doesn't have a Dot yet. Dots are rolling out to ChatGPT Pro, Business Premium and Enterprise, but not in every region. Create yours at chatgpt.com/dots, then connect again."))
	case httpErr != nil && httpErr.Status == http.StatusUnauthorized:
		mapped = bridgev2.RespError(mautrix.MForbidden.WithMessage("Your ChatGPT session expired. Sign in again."))
	case httpErr != nil && httpErr.Challenge:
		mapped = bridgev2.RespError(mautrix.MUnknown.WithMessage("ChatGPT's Cloudflare check blocked the bridge. Try again in a few minutes."))
	case httpErr != nil && httpErr.Status == http.StatusForbidden:
		mapped = bridgev2.RespError(mautrix.MForbidden.WithMessage("ChatGPT denied access while setting up Dots. Try again in a moment."))
	case httpErr != nil && httpErr.Status == http.StatusTooManyRequests:
		mapped = bridgev2.RespError(mautrix.MLimitExceeded.WithMessage("ChatGPT is rate limiting requests. Wait a moment and try again.").WithStatus(http.StatusBadRequest))
	case httpErr != nil && httpErr.Status >= 500:
		mapped = bridgev2.RespError(mautrix.MUnknown.WithMessage("ChatGPT is having server problems. Try again later."))
	case errors.Is(err, chatgpt.ErrNoPrimaryDot):
		mapped = bridgev2.RespError(mautrix.MBadState.WithMessage("No Dot is selected in ChatGPT. Pick your Dot at chatgpt.com/dots, then connect again.").WithStatus(http.StatusBadRequest))
	case errors.Is(err, errNoBaseline):
		mapped = bridgev2.RespError(mautrix.MBadState.WithMessage("Send a message to your Dot in ChatGPT first, then connect again.").WithStatus(http.StatusBadRequest))
	case isTransportError(err):
		mapped = bridgev2.RespError(mautrix.MUnknown.WithMessage("Couldn't reach ChatGPT. Try again."))
	default:
		mapped = bridgev2.RespError(mautrix.MUnknown.WithMessage("ChatGPT Dots setup failed. Retry setup, or contact Beeper support if this continues."))
	}
	return fmt.Errorf("%w: %w", mapped, err)
}
