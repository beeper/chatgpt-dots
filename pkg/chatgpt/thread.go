package chatgpt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

type ThreadStatus struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Status    string `json:"status"`
	TurnID    string `json:"turn_id,omitempty"`
	StartedAt int64  `json:"started_at,omitempty"`
	UpdatedAt int64  `json:"updated_at,omitempty"`
}

func (t *ThreadStatus) Terminal() bool {
	return t != nil && (t.Status == "completed" || t.Status == "failed" || t.Status == "interrupted")
}

const taskOrigin = "https://codex-cloud-backend.chatgpt.com"

func (c *Client) checkTaskRef(room string, message Message, threadID string) error {
	c.mu.RLock()
	verified := c.rooms[room]
	c.mu.RUnlock()
	found := false
	for _, raw := range message.Content.Attachments {
		var ref struct {
			Type string `json:"type"`
			ID   string `json:"thread_id"`
		}
		if json.Unmarshal(raw, &ref) == nil && ref.Type == "thread" && ref.ID == threadID {
			found = true
		}
	}
	if !verified || message.Deleted != nil || message.ID == "" || threadID == "" || !found {
		return errors.New("refusing task without a verified Dot room attachment")
	}
	return nil
}

func (c *Client) Thread(ctx context.Context, room string, message Message, threadID string) (*ThreadStatus, error) {
	if err := c.checkTaskRef(room, message, threadID); err != nil {
		return nil, err
	}
	var metadata struct {
		Thread struct {
			ID        string `json:"id"`
			Name      string `json:"name"`
			UpdatedAt int64  `json:"updatedAt"`
			Status    struct {
				Type        string            `json:"type"`
				ActiveFlags []json.RawMessage `json:"activeFlags"`
			} `json:"status"`
		} `json:"thread"`
	}
	if err := c.doAt(ctx, taskOrigin, "GET", "/v1/threads/"+url.PathEscape(threadID), nil, &metadata); err != nil {
		return nil, err
	}
	if metadata.Thread.ID != threadID || metadata.Thread.Status.Type == "" {
		return nil, errors.New("ChatGPT task response has no matching identity or status")
	}
	var turns struct {
		Data []struct {
			ID        string `json:"id"`
			Status    string `json:"status"`
			StartedAt int64  `json:"startedAt"`
		} `json:"data"`
	}
	if err := c.doAt(ctx, taskOrigin, "GET", "/v2/threads/"+url.PathEscape(threadID)+"/turns?limit=1&sortDirection=desc&itemsView=notLoaded", nil, &turns); err != nil {
		return nil, err
	}
	result := &ThreadStatus{ID: threadID, Title: strings.TrimSpace(metadata.Thread.Name), UpdatedAt: metadata.Thread.UpdatedAt, Status: "thinking"}
	if len(turns.Data) > 0 {
		result.TurnID = turns.Data[0].ID
		result.StartedAt = turns.Data[0].StartedAt
		result.Status = turns.Data[0].Status
	}
	switch metadata.Thread.Status.Type {
	case "systemError":
		result.Status = "failed"
	case "active":
		result.Status = "inProgress"
		if len(metadata.Thread.Status.ActiveFlags) > 0 {
			result.Status = "needsInput"
		}
	}
	switch result.Status {
	case "thinking", "inProgress", "needsInput", "completed", "failed", "interrupted":
		return result, nil
	default:
		return nil, errors.New("ChatGPT returned an unknown task turn state")
	}
}

type TaskItem struct {
	ID        string
	FromUser  bool
	Text      string
	Timestamp time.Time
}

const maxTaskTurns = 20

var taskItemViews = []string{"summary", "full"}

func (c *Client) TaskItems(ctx context.Context, room string, message Message, threadID string) ([]TaskItem, error) {
	if err := c.checkTaskRef(room, message, threadID); err != nil {
		return nil, err
	}
	c.mu.RLock()
	first := c.taskItemsView
	c.mu.RUnlock()
	var err error
	for _, view := range taskItemViews[first:] {
		var items []TaskItem
		if items, err = c.taskItems(ctx, threadID, view); err == nil {
			return items, nil
		}
		err = fmt.Errorf("itemsView=%s: %w", view, err)
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) || httpErr.Status != http.StatusBadRequest && httpErr.Status != http.StatusUnprocessableEntity {
			return nil, err
		}
		c.mu.Lock()
		c.taskItemsView = min(c.taskItemsView+1, len(taskItemViews)-1)
		c.mu.Unlock()
	}
	return nil, err
}

func (c *Client) taskItems(ctx context.Context, threadID, view string) ([]TaskItem, error) {
	type turn struct {
		ID          string            `json:"id"`
		Status      string            `json:"status"`
		Items       []json.RawMessage `json:"items"`
		StartedAt   int64             `json:"startedAt"`
		CompletedAt int64             `json:"completedAt"`
	}
	var turns []turn
	cursor := ""
	seen := make(map[string]bool)
	for len(turns) < maxTaskTurns {
		query := url.Values{"limit": {"1"}, "sortDirection": {"desc"}, "itemsView": {view}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var page struct {
			Data       []turn `json:"data"`
			NextCursor string `json:"nextCursor"`
		}
		if err := c.doAt(ctx, taskOrigin, "GET", "/v2/threads/"+url.PathEscape(threadID)+"/turns?"+query.Encode(), nil, &page); err != nil {
			return nil, err
		}
		turns = append(turns, page.Data...)
		if page.NextCursor == "" || seen[page.NextCursor] {
			break
		}
		seen[page.NextCursor] = true
		cursor = page.NextCursor
	}
	slices.Reverse(turns)
	var result []TaskItem
	for _, t := range turns {
		finished := t.Status == "completed" || t.Status == "failed" || t.Status == "interrupted"
		var prompt, answer *TaskItem
		answerFinal := false
		for _, raw := range t.Items {
			var item struct {
				Type    string `json:"type"`
				ID      string `json:"id"`
				Text    string `json:"text"`
				Phase   string `json:"phase"`
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
				StartedAtMs   int64 `json:"startedAtMs"`
				CompletedAtMs int64 `json:"completedAtMs"`
			}
			if json.Unmarshal(raw, &item) != nil || item.ID == "" {
				continue
			}
			out := TaskItem{ID: item.ID}
			switch {
			case item.Type == "userMessage" && prompt == nil:
				var parts []string
				for _, part := range item.Content {
					if part.Type == "text" && strings.TrimSpace(part.Text) != "" {
						parts = append(parts, part.Text)
					}
				}
				out.FromUser, out.Text = true, strings.Join(parts, "\n")
			case item.Type == "agentMessage" && finished && !answerFinal:
				out.Text = item.Text
			default:
				continue
			}
			if strings.TrimSpace(out.Text) == "" {
				continue
			}
			switch {
			case item.CompletedAtMs > 0:
				out.Timestamp = time.UnixMilli(item.CompletedAtMs)
			case item.StartedAtMs > 0:
				out.Timestamp = time.UnixMilli(item.StartedAtMs)
			case !out.FromUser && t.CompletedAt > 0:
				out.Timestamp = time.Unix(t.CompletedAt, 0)
			case t.StartedAt > 0:
				out.Timestamp = time.Unix(t.StartedAt, 0)
			}
			if out.FromUser {
				prompt = &out
			} else {
				answer = &out
				answerFinal = item.Phase == "final_answer" || item.Phase == "finalAnswer"
			}
		}
		for _, item := range []*TaskItem{prompt, answer} {
			if item != nil {
				result = append(result, *item)
			}
		}
	}
	return result, nil
}
