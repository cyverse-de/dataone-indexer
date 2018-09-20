package model

import (
	"encoding/json"
	"time"
)

// User represents an iRODS qualified username.
type User struct {
	Name string `json:"name"`
	Zone string `json:"zone"`
}

// Timestamp represents the time an event occurred.
type Timestamp time.Time

// ReferenceTime represents the timestamp format used by the service.
const ReferenceTime = "\"2006-01-02.15:04:05\""

// CurrentTimestamp returns a timestamp representing the current time.
func CurrentTimestamp() *Timestamp {
	t := time.Now()
	return (*Timestamp)(&t)
}

// UnmarshalJSON converts serialized JSON to a structure.
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

// ToTime conversts a timestamp to a time pointer.
func (ts *Timestamp) ToTime() *time.Time {
	return (*time.Time)(ts)
}

// Message represents an event message sent from iRODS.
type Message struct {
	Author    *User      `json:"author"`
	Entity    string     `json:"entity"`
	Path      string     `json:"path"`
	Timestamp *Timestamp `json:"timestamp,omitempty"`
}

// Decode converts a serialized JSON message to a structure.
func Decode(body []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
