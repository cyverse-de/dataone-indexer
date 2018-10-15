package database

import (
	"database/sql"
	"testing"

	"github.com/cyverse-de/dataone-indexer/model"
)

// Routing keys to use during testing.
const (
	RKRead = "read-event"
	RKFake = "fake-event"
)

// CallMap represents a  structure that records calls to handler functions.
type CallMap struct {
	Read int32
}

// newCallMap returns a fresh call map.
func newCallMap() *CallMap {
	return &CallMap{
		Read: 0,
	}
}

// MockRecorder is a fake event recorder used to test the dispatch system.
type MockRecorder struct {
	handlers *HandlerMap
	nodeID   string
	callMap  *CallMap
}

// GetNodeID returns the node identifier associated with a mock event recorder.
func (r MockRecorder) GetNodeID() string {
	return r.nodeID
}

// GetDb always returns nil. We don't need a database connection to test the dispatch system.
func (r MockRecorder) GetDb() *sql.DB {
	return nil
}

// GetHandlerMap returns a map from routing key to message handler function.
func (r MockRecorder) GetHandlerMap() *HandlerMap {
	return r.handlers
}

// RecordEvent records an event in the database if there is a handler for the given routing key.
func (r MockRecorder) RecordEvent(key string, msg *model.Message) error {
	return dispatchMessage(r, key, msg)
}

// newMockRecorder returns a mock event recorder with default settings.
func newMockRecorder() *MockRecorder {
	var r MockRecorder
	r = MockRecorder{
		handlers: &HandlerMap{
			RKRead: func(recorder Recorder, key string, msg *model.Message) error {
				r.callMap.Read++
				return nil
			},
		},
		nodeID:  "some-node",
		callMap: newCallMap(),
	}
	return &r
}

// TestDispatch verifies that messages are dispatched as expected.
func TestDispatch(t *testing.T) {
	r := newMockRecorder()
	r.RecordEvent(RKRead, nil)

	// The method to record read events should have been called.
	if r.callMap.Read != 1 {
		t.Error("the function to record read events should have been called and was not")
	}
}

// TestNonDispatch verifies that messages that should not be dispatched are not.
func TestNonDispatch(t *testing.T) {
	r := newMockRecorder()
	r.RecordEvent(RKFake, nil)

	// The method to record read events should not have been called.
	if r.callMap.Read != 0 {
		t.Error("the function to record read events was called when it should not have been")
	}
}
