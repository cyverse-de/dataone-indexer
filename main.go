// dataone-indexer
//
// This service listens to the AMQP exchange for the CyVerse data store for events that are relevant to the DataONE
// member node service and records them in the DataONE event database.
package main

import (
	"database/sql"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/dataone-indexer/logger"
	"github.com/cyverse-de/dbutil"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"gopkg.in/alecthomas/kingpin.v2"
)

var defaultConfig = `
amqp:
  uri: amqp://guest:guest@rabbit:5672/jobs
  exchange:
    name: de
    durable: true
    auto-delete: false

db:
  uri: postgresql://guest:guest@dedb:5432/de?sslmode=disable
`

// Command-line option definitions.
var (
	config = kingpin.Flag("config", "Path to configuration file.").Short('c').Required().File()
)

type Messenger interface {
}

type DataoneIndexer struct {
	cfg        *viper.Viper
	amqpClient *Messenger
	db         *sql.DB
}

// Establishes a connection to the DE database.
func getDbConnection(dburi string) (*sql.DB, error) {

	connector, err := dbutil.NewDefaultConnector("1m")
	if err != nil {
		return nil, err
	}

	db, err := connector.Connect("postgres", dburi)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Initializes the DataONE indexer service.
func initService() *DataoneIndexer {

	// Parse the command-line options.
	kingpin.Parse()

	// Load the configuration file.
	cfg, err := configurate.InitDefaultsR(*config, configurate.JobServicesDefaults)
	if err != nil {
		logger.Log.Fatalf("Unable to load the configuration: %s", err)
	}

	// Establish the database connection.
	db, err := getDbConnection(cfg.GetString("db.uri"))
	if err != nil {
		logger.Log.Fatalf("Unable to establish the database connection: %s", err)
	}

}

func main() {
	svc := initService()
}
