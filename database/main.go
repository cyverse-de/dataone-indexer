package database

import (
	"database/sql"

	"github.com/cyverse-de/dataone-indexer/model"
)

// Recorder is an interface for recording DataONE events.
type Recorder interface {
	RecordEvent(key string, msg *model.Message) error
	GetNodeId() string
	GetDb() *sql.DB
}

// DefaultRecordes is an implementation of the Recorder interface that stores DataONE events in a database.
type DefaultRecorder struct {
	db       *sql.DB
	handlers *map[string]func(Recorder, string, *model.Message) error
	nodeId   string
}

// KeyNames represents a mapping from DataONE event type to AMQP routing keys.
type KeyNames struct {
	Read string
}

// recordReadEvent is the function that DefaultRecorder uses to record file accesses.
func recordReadEvent(r Recorder, key string, msg *model.Message) error {

	// Begin a transaction.
	tx, err := r.GetDb().Begin()
	if err != nil {
		return err
	}

	// Insert the row into the database.
	_, err = tx.Exec(addEvent, msg.Entity, msg.Path, ET_READ, msg.Timestamp.ToTime(), r.GetNodeId())
	if err != nil {
		tx.Rollback()
		return err
	}

	// Commit the transaction.
	return tx.Commit()
}

// buildHandlerMap builds a map from AMQP routing key to handler functions.
func buildHandlerMap(keyNames *KeyNames) *map[string]func(Recorder, string, *model.Message) error {
	return &map[string]func(Recorder, string, *model.Message) error{
		keyNames.Read: recordReadEvent,
	}
}

// NewRecorder creates and returns a new DefaultRecorder object.
func NewRecorder(db *sql.DB, keyNames *KeyNames, nodeId string) *DefaultRecorder {
	return &DefaultRecorder{
		db:       db,
		handlers: buildHandlerMap(keyNames),
		nodeId:   nodeId,
	}
}

// GetNodeId returns the node ID associated with a DefaultRecorder.
func (r DefaultRecorder) GetNodeId() string {
	return r.nodeId
}

// GetDb returns the database connection associated with a DefaultHandler.
func (r DefaultRecorder) GetDb() *sql.DB {
	return r.db
}

// RecordEvent records an event in the database if there is a handler for the given routing key.
func (r DefaultRecorder) RecordEvent(key string, msg *model.Message) error {
	if f := (*r.handlers)[key]; f != nil {
		return f(r, key, msg)
	}
	return nil
}
