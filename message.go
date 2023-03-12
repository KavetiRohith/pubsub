package main

import (
	"encoding/json"
)

type WsMessage struct {
	Action  string           `json:"action"`
	Topic   string           `json:"topic"`
	Message *json.RawMessage `json:"message,omitempty"`
}

type PublishMessage struct {
	Message *json.RawMessage `json:"message"`
	Topic   string           `json:"topic"`
}
