package model

import (
	"testing"
	"time"
)

var noTimestamp = []byte(`
{
  "author": {
    "name": "nobody",
    "zone": "nowhere"
  },
  "entity": "fakeid",
  "path": "/foo/bar"
}
`)

func validateCommonFields(t *testing.T, msg *Message) {
	if msg.Author == nil {
		t.Error("no author in decoded message")
	}
	if msg.Author.Name != "nobody" {
		t.Errorf("expected author name `nobody` but got `%s`", msg.Author.Name)
	}
	if msg.Author.Zone != "nowhere" {
		t.Errorf("expected author zone `nowhere` but got `%s`", msg.Author.Zone)
	}
	if msg.Entity != "fakeid" {
		t.Errorf("expected entity ID `fakeid` but got `%s`", msg.Entity)
	}
	if msg.Path != "/foo/bar" {
		t.Errorf("expected path `/foo/bar` but got `%s`", msg.Path)
	}
}

func TestNoTimestamp(t *testing.T) {
	msg, err := Decode(noTimestamp)
	if err != nil {
		t.Fatalf("error encountered while decoding message: %s", err)
	}

	validateCommonFields(t, msg)
	if msg.Timestamp != nil {
		t.Errorf("expected nil timestamp")
	}
}

var withTimestamp = []byte(`
{
  "author": {
    "name": "nobody",
    "zone": "nowhere"
  },
  "entity": "fakeid",
  "path": "/foo/bar",
  "timestamp": "2017-10-06.15:07:37"
}
`)

func TestTimestamp(t *testing.T) {
	msg, err := Decode(withTimestamp)
	if err != nil {
		t.Fatalf("error encoutnered while decoding message: %s", err)
	}

	validateCommonFields(t, msg)
	if msg.Timestamp == nil {
		t.Fatal("no timestamp extracted from message")
	}
	expectedTimestamp := "\"2017-10-06.15:07:37\""
	expectedTime, err := time.Parse(ReferenceTime, expectedTimestamp)
	if err != nil {
		t.Fatalf("unable to parse expected timestamp: %s", err)
	}
	actualTime := time.Time(*msg.Timestamp)
	if actualTime != expectedTime {
		t.Errorf("expected timestamp of `%s` but got `%s`", expectedTimestamp, expectedTime.Format(ReferenceTime))
	}
}

var extraFields = []byte(`
{
  "author": {
    "name": "nobody",
    "zone": "nowhere"
  },
  "entity": "fakeid",
  "path": "/foo/bar",
  "creator": {
    "name": "brucealmighty",
    "zone": "nowhere"
  }
}
`)

func TestExtraFields(t *testing.T) {
	msg, err := Decode(extraFields)
	if err != nil {
		t.Fatalf("error encountered while decoding message: %s", err)
	}

	validateCommonFields(t, msg)
}
