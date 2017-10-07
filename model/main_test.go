package model

import (
	"testing"
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
		t.Errorf("expected nil timestamp but got `%s`", *msg.Timestamp)
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
	if *msg.Timestamp != "2017-10-06.15:07:37" {
		t.Errorf("expected timestamp of `2017-10-06.15:07:37` but got `%s`", *msg.Timestamp)
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
