package database

import (
	"database/sql"

	"github.com/cyverse-de/dataone-indexer/model"
)

type Recorder interface {
	RecordEvent(key string, msg *model.Message) error
	GetNodeId() string
}

type DefaultRecorder struct {
	db       *sql.DB
	handlers *map[string]func(Recorder, string, *model.Message) error
	nodeId   string
}

type KeyNames struct {
	Read string
}

func recordReadEvent(r Recorder, key string, msg *model.Message) error {

	// Insert the row into the database.

	return nil
}

func buildHandlerMap(keyNames *KeyNames) *map[string]func(Recorder, string, *model.Message) error {
	return &map[string]func(Recorder, string, *model.Message) error{
		keyNames.Read: recordReadEvent,
	}
}

func newRecorder(db *sql.DB, keyNames *KeyNames, nodeId string) *DefaultRecorder {
	return &DefaultRecorder{
		db:       db,
		handlers: buildHandlerMap(keyNames),
		nodeId:   nodeId,
	}
}

func (r DefaultRecorder) GetNodeId() string {
	return r.nodeId
}

func (r DefaultRecorder) RecordEvent(key string, msg *model.Message) error {
	if f := (*r.handlers)[key]; f != nil {
		return f(r, key, msg)
	}
	return nil
}
