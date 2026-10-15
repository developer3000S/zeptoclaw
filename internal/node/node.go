// Package node assembles every mesh component into one running node and owns
// its lifecycle: start, maintenance, graceful stop.
package node

import (
	"context"
	"github.com/developer3000S/zeptoclaw/internal/tasks"
	"github.com/developer3000S/zeptoclaw/internal/chat"
)

// SubmitRequest is re-exported so callers outside the tasks package (the admin
// API, the CLI) do not need to import it.
type SubmitRequest = tasks.SubmitRequest

// ChatRequest represents a chat message request
type ChatRequest struct {
	Message string `json:"message"`
	UserID  string `json:"user_id"`
}

// ChatResponse represents a chat message response
type ChatResponse struct {
	Response string `json:"response"`
	Timestamp int64 `json:"timestamp"`
}

// ChatHistory represents the chat history
type ChatHistory struct {
	Messages []ChatMessage `json:"messages"`
}

// ChatMessage represents a single chat message
type ChatMessage struct {
	Message   string `json:"message"`
	UserID    string `json:"user_id"`
	Timestamp int64  `json:"timestamp"`
}

// Node is the main node structure that includes chat functionality
type Node struct {
	// ... existing fields ...
	ChatManager *chat.ChatManager
}

// NewNode creates a new node with chat functionality
func NewNode() *Node {
	return &Node{
		ChatManager: chat.NewChatManager(),
	}
}

// HandleChat processes a chat message
func (n *Node) HandleChat(ctx context.Context, req ChatRequest) (ChatResponse, error) {
	// Process the chat message
	n.ChatManager.AddMessage(req.Message, req.UserID)

	// Generate a response
	response := ChatResponse{
		Response: "Message received",
		Timestamp: time.Now().Unix(),
	}

	return response, nil
}

// GetChatHistory returns the chat history
func (n *Node) GetChatHistory() ChatHistory {
	return n.ChatManager.GetHistory()
}
