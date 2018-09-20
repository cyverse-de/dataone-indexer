package logger

import (
	"github.com/sirupsen/logrus"
)

// Log is a logger with several service specific fields.
var Log = logrus.WithFields(logrus.Fields{
	"services": "dataone-indexer",
	"art-id":   "dataone-indexer",
	"group":    "org.cyverse",
})

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
}
