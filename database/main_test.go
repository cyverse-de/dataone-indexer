package database

import (
	"database/sql"
	"testing"

	"github.com/cyverse-de/dataone-indexer/model"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

const (
	READ_KEY = "data-object.open"
)

func getKeyNames() *KeyNames {
	return &KeyNames{
		Read: READ_KEY,
	}
}

func getTestRecorder(db *sql.DB) Recorder {
	return NewRecorder(db, getKeyNames(), "fakenode")
}

func getTestMessage() *model.Message {
	return &model.Message{
		Author:    &model.User{Name: "ipcdev", Zone: "iplant"},
		Entity:    "F3579BF9-284B-4B3C-841B-F6E87D3F78EA",
		Path:      "/iplant/home/shared/commons-repo/curated/foo.txt",
		Timestamp: model.CurrentTimestamp(),
	}
}

func TestReadEvent(t *testing.T) {

	// Create the stub database connection.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening stub databasse connection: %s", err)
	}

	// Prepare to record the message.
	r := getTestRecorder(db)
	msg := getTestMessage()

	// Describe the expected database actions.
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO events").WithArgs(
		msg.Entity, msg.Path, ET_READ, msg.Timestamp.ToTime(), r.GetNodeId(),
	).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Record the message.
	if err := r.RecordEvent(READ_KEY, msg); err != nil {
		t.Fatalf("error encountered while recording event: %s", err)
	}

	// Verify that the expectations were met.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
