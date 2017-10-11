package database

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/cyverse-de/dataone-indexer/model"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

// Routing keys to use for testing.
const (
	READ_KEY = "data-object.open"
)

// getKeyNames defines the structure describing which routing keys correspond to which types of events.
func getKeyNames() *KeyNames {
	return &KeyNames{
		Read: READ_KEY,
	}
}

// getTestRecorder returns a default event recorder that can be used for these tests.
func getTestRecorder(db *sql.DB) Recorder {
	return NewRecorder(db, getKeyNames(), "fakenode")
}

// getTestMessage returns a message that can be used for testing.
func getTestMessage() *model.Message {
	return &model.Message{
		Author:    &model.User{Name: "ipcdev", Zone: "iplant"},
		Entity:    "F3579BF9-284B-4B3C-841B-F6E87D3F78EA",
		Path:      "/iplant/home/shared/commons-repo/curated/foo.txt",
		Timestamp: model.CurrentTimestamp(),
	}
}

// TestReadEvent verifies that a read event can be recorded successfully.
func TestReadEvent(t *testing.T) {

	// Create the stub database connection.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening stub database connection: %s", err)
	}

	// Prepare to record the message.
	r := getTestRecorder(db)
	msg := getTestMessage()

	// Describe the expected database actions.
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO events").
		WithArgs(msg.Entity, msg.Path, ET_READ, msg.Timestamp.ToTime(), r.GetNodeId()).
		WillReturnResult(sqlmock.NewResult(1, 1))
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

// TestErroneousReadEvent verifies that database errors that occur while recording a read event are handled correctly.
func TestErroneousReadEvent(t *testing.T) {

	// Create the stub database connection.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening stub database connection: %s", err)
	}

	// Prepare to record the message.
	r := getTestRecorder(db)
	msg := getTestMessage()

	// Describe the expected database actions.
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO events").
		WithArgs(msg.Entity, msg.Path, ET_READ, msg.Timestamp.ToTime(), r.GetNodeId()).
		WillReturnError(fmt.Errorf("something bad happened"))
	mock.ExpectRollback()

	// Record the message.
	if err := r.RecordEvent(READ_KEY, msg); err == nil {
		t.Fatalf("an error was expected but none was encountered")
	}

	// Verify that the expectations were met.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
