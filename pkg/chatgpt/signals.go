package chatgpt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

type ReadReceipt struct {
	Member  string    `json:"account_user_id"`
	ReadAt  time.Time `json:"last_read_at"`
	Updated time.Time `json:"read_cursor_updated_at"`
}

type Signal struct {
	Type    string `json:"type"`
	Payload struct {
		Room      string  `json:"room_id"`
		Typing    *bool   `json:"typing"`
		Timestamp float64 `json:"timestamp"`
		Source    struct {
			Role   string `json:"role"`
			Member string `json:"account_user_id"`
		} `json:"source"`
		Message struct {
			Member string `json:"account_user_id"`
		} `json:"message"`
	} `json:"payload"`
}

func (c *Client) RoomSignals(ctx context.Context, p Profile) (*Room, error) {
	c.mu.RLock()
	verified := c.rooms[p.Room]
	c.mu.RUnlock()
	if !verified {
		return nil, errors.New("refusing unverified Dot room")
	}
	var room Room
	if err := c.do(ctx, "GET", "/messaging/rooms/"+url.PathEscape(p.Room), nil, &room); err != nil {
		return nil, err
	}
	if room.ID != p.Room || room.Aeon != p.ID || room.Type != "DM" || room.Source != "chatgpt:messaging" {
		return nil, errors.New("Dot room signal identity changed")
	}
	return &room, nil
}

type StreamDisconnectedError struct {
	CloseStatus websocket.StatusCode
	PingFailed  bool
	Subscribed  bool
	Connected   time.Duration
	ReadError   string
}

func (StreamDisconnectedError) Error() string { return "ChatGPT messaging stream disconnected" }

func readErrorKind(err error) string {
	var netErr net.Error
	var opErr *net.OpError
	switch {
	case websocket.CloseStatus(err) != -1:
		return "close_frame"
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, net.ErrClosed):
		return "closed"
	case errors.As(err, &netErr) && netErr.Timeout():
		return "net_timeout"
	case errors.As(err, &opErr):
		return "net_" + opErr.Op
	}
	for inner := errors.Unwrap(err); inner != nil; inner = errors.Unwrap(err) {
		err = inner
	}
	return fmt.Sprintf("%T", err)
}

func keepAlive(ctx context.Context, conn *websocket.Conn, failed *atomic.Bool) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := conn.Ping(pingCtx)
		cancel()
		if err != nil {
			failed.Store(true)
			conn.CloseNow()
			return
		}
	}
}

func (c *Client) StreamSignals(ctx context.Context, handle func(Signal) error) error {
	var endpoint struct {
		URL string `json:"websocket_url"`
	}
	if err := c.do(ctx, "GET", "/celsius/ws/user", nil, &endpoint); err != nil {
		return err
	}
	u, err := url.Parse(endpoint.URL)
	if err != nil {
		return errors.New("invalid ChatGPT messaging socket URL")
	}
	if u.Scheme != "wss" || u.User != nil || u.Host != "ws.chatgpt.com" {
		return fmt.Errorf("unexpected ChatGPT messaging socket origin %s://%s", u.Scheme, u.Host)
	}
	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	conn, _, err := websocket.Dial(dialCtx, endpoint.URL, &websocket.DialOptions{HTTPClient: &http.Client{Transport: c.http.Transport, CheckRedirect: c.http.CheckRedirect}})
	cancel()
	if err != nil {
		return errors.New("ChatGPT messaging socket connection failed")
	}
	defer conn.CloseNow()
	conn.SetReadLimit(4 << 20)
	connected := time.Now()
	var pingFailed atomic.Bool
	pingCtx, stopPing := context.WithCancel(ctx)
	defer stopPing()
	go keepAlive(pingCtx, conn, &pingFailed)
	commands := `[{"id":1,"command":{"type":"connect","presence":{"type":"presence","state":"background"}}},{"id":2,"command":{"type":"subscribe","topic_id":"calpico-chatgpt-messaging"}}]`
	writeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	err = conn.Write(writeCtx, websocket.MessageText, []byte(commands))
	cancel()
	if err != nil {
		return errors.New("ChatGPT messaging subscription failed")
	}
	subscribed := false
	for ctx.Err() == nil {
		readCtx := ctx
		cancel := func() {}
		if !subscribed {
			readCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
		}
		_, data, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			return StreamDisconnectedError{CloseStatus: websocket.CloseStatus(err), PingFailed: pingFailed.Load(), Subscribed: subscribed, Connected: time.Since(connected), ReadError: readErrorKind(err)}
		}
		var frames []struct {
			ID      int             `json:"id"`
			Type    string          `json:"type"`
			Topic   string          `json:"topic_id"`
			Payload json.RawMessage `json:"payload"`
			Reply   struct {
				Type  string `json:"type"`
				Topic string `json:"topic_id"`
			} `json:"reply"`
		}
		if json.Unmarshal(data, &frames) != nil {
			continue
		}
		for _, frame := range frames {
			if frame.ID == 2 {
				if frame.Reply.Type != "subscribe" || frame.Reply.Topic != "calpico-chatgpt-messaging" {
					return errors.New("ChatGPT messaging subscription rejected")
				}
				subscribed = true
				if err := handle(Signal{Type: "resync"}); err != nil {
					return err
				}
				continue
			}
			if !subscribed || frame.Type != "message" || frame.Topic != "calpico-chatgpt-messaging" {
				continue
			}
			var signal Signal
			if json.Unmarshal(frame.Payload, &signal) != nil {
				continue
			}
			c.mu.RLock()
			verified := c.rooms[signal.Payload.Room]
			c.mu.RUnlock()
			if verified {
				if err := handle(signal); err != nil {
					return err
				}
			}
		}
	}
	return ctx.Err()
}
