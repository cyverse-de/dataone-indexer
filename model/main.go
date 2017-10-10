package model

import (
	"encoding/json"
	"time"
)

type User struct {
	Name string `json:"name"`
	Zone string `json:"zone"`
}

type Timestamp time.Time

const ReferenceTime = "\"2006-01-02.15:04:05\""

func (ts *Timestamp) UnmarshalJSON(value []byte) error {

	// Handle the null constant.
	if string(value) == "null" {
		return nil
	}

	// Parse the timestamp.
	t, err := time.Parse(ReferenceTime, string(value))
	*ts = Timestamp(t)
	return err
}

type Message struct {
	Author    *User      `json:"author"`
	Entity    string     `json:"entity"`
	Path      string     `json:"path"`
	Timestamp *Timestamp `json:"timestamp,omitempty"`
}

func Decode(body []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
