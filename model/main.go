package model

import (
	"encoding/json"
)

type User struct {
	Name string `json:"name"`
	Zone string `json:"zone"`
}

type Message struct {
	Author    *User   `json:"author"`
	Entity    string  `json:"entity"`
	Path      string  `json:"path"`
	Timestamp *string `json:"timestamp,omitempty"`
}

func Decode(body []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
