package database

import (
	"database/sql"

	"github.com/cyverse-de/dataone-indexer/model"
)

type Recorder interface {
	RecordEvent(key string, msg *model.Message)
}

type DefaultRecorder struct {
	db       *sql.DB
	handlers *map[string]func(*sql.DB, string, *model.Message) error
}

type KeyNames struct {
	Read string
}

func recordReadEvent(db *sql.DB, key string, msg *model.Message) error {
	return nil
}

func buildHandlerMap(keyNames *KeyNames) *map[string]func(*sql.DB, string, *model.Message) error {
	return &map[string]func(*sql.DB, string, *model.Message) error{
		keyNames.Read: recordReadEvent,
	}
}

func newRecorder(db *sql.DB, keyNames *KeyNames) *DefaultRecorder {
	return &DefaultRecorder{
		db:       db,
		handlers: buildHandlerMap(keyNames),
	}
}

func (r *DefaultRecorder) RecordEvent(key string, msg *model.Message) error {
	return nil
}
