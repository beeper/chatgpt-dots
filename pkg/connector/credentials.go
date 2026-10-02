package connector

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/beeper/chatgpt-dots/pkg/chatgpt"
	"maunium.net/go/mautrix/bridgev2"
)

var errCredentialStorage = errors.New("could not access ChatGPT session storage")

func (c *Connector) readCredentials(_ context.Context, login *bridgev2.UserLogin) (chatgpt.Credentials, error) {
	meta := login.Metadata.(*LoginMetadata)
	if meta.Credentials == nil {
		return chatgpt.Credentials{}, errors.New("ChatGPT session is not stored in this bridge database; reconnect with ChatGPT")
	}
	if err := validateCredentials(meta, *meta.Credentials); err != nil {
		return chatgpt.Credentials{}, err
	}
	return *meta.Credentials, nil
}

func validateCredentials(meta *LoginMetadata, credentials chatgpt.Credentials) error {
	identity, err := credentials.Identity()
	if err != nil || identity.Account != meta.Account || identity.User != meta.User {
		return errCredentialStorage
	}
	body, err := json.Marshal(credentials)
	defer clear(body)
	if err != nil || len(body) > 65536 {
		return errCredentialStorage
	}
	return nil
}

func (c *Connector) writeCredentials(ctx context.Context, login *bridgev2.UserLogin, credentials chatgpt.Credentials) error {
	meta := login.Metadata.(*LoginMetadata)
	if err := validateCredentials(meta, credentials); err != nil {
		return err
	}
	previous := meta.Credentials
	meta.Credentials = &credentials
	if err := login.Save(ctx); err != nil {
		meta.Credentials = previous
		return errCredentialStorage
	}
	return nil
}

func (c *Connector) removeCredentials(ctx context.Context, login *bridgev2.UserLogin) error {
	meta := login.Metadata.(*LoginMetadata)
	previous := meta.Credentials
	meta.Credentials = nil
	if err := login.Save(ctx); err != nil {
		meta.Credentials = previous
		return errors.New("could not remove ChatGPT credentials from bridge session storage")
	}
	return nil
}
