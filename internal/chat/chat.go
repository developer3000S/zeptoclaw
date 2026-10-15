package chat

import (
	"time"
)

// ChatManager handles chat operations

type ChatManager struct {
	messages []ChatMessage
}

// NewChatManager creates a new ChatManager
func NewChatManager() *ChatManager {
	return &ChatManager{
		messages: make([]ChatMessage, 0),
	}
}

// AddMessage adds a new message to the chat history
func (cm *ChatManager) AddMessage(message, userID string) {
	cm.messages = append(cm.messages, ChatMessage{
		Message:   message,
		UserID:    userID,
		Timestamp: time.Now().Unix(),
	})
}

// GetHistory returns the chat history
func (cm *ChatManager) GetHistory() ChatHistory {
	return ChatHistory{
		Messages: cm.messages,
	}
}

// ChatMessage represents a single chat message
type ChatMessage struct {
	Message   string `json:"message"`
	UserID    string `json:"user_id"`
	Timestamp int64  `json:"timestamp"`
}

// ChatHistory represents the chat history
type ChatHistory struct {
	Messages []ChatMessage `json:"messages"`
}