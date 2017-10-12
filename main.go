// dataone-indexer
//
// This service listens to the AMQP exchange for the CyVerse data store for events that are relevant to the DataONE
// member node service and records them in the DataONE event database.
package main

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/dataone-indexer/database"
	"github.com/cyverse-de/dataone-indexer/logger"
	"github.com/cyverse-de/dataone-indexer/model"
	"github.com/cyverse-de/dbutil"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"gopkg.in/alecthomas/kingpin.v2"
)

var defaultConfig = `
amqp:
  uri: amqp://guest:guest@rabbit:5672/jobs
  exchange:
    name: de

db:
  uri: postgresql://guest:guest@dedb:5432/de?sslmode=disable

dataone:
  repository-root: /iplant/home/shared/commons_repo/curated
  node-id: foo
  amqp-routing-keys:
    read: data-object.open
`

// Command-line option definitions.
var (
	config = kingpin.Flag("config", "Path to configuration file.").Short('c').Required().File()
)

// DataoneIndexer represents this service.
type DataoneIndexer struct {
	cfg      *viper.Viper
	messages <-chan amqp.Delivery
	db       *sql.DB
	rootDir  string
	recorder database.Recorder
}

// getDbConnection establishes a connection to the DataONE event database.
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

// getAmqpChannel establishes a connection to the AMQP Broker and returns a channel to use for receiving messages.
func getAmqpChannel(cfg *viper.Viper) (<-chan amqp.Delivery, error) {
	uri := cfg.GetString("amqp.uri")
	exchange := cfg.GetString("amqp.exchange.name")
	queueName := "dataone.events"
	routingKeys := cfg.GetStringMapString("dataone.amqp-routing-keys")

	// Establish the AMQP connection.
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, err
	}

	// Create the AMQP channel.
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// Declare the queue.
	queue, err := ch.QueueDeclare(
		queueName, // queue name
		true,      // queue durable
		false,     // queue auto-delete flag
		false,     // queue exclusive flag
		false,     // queue no-wait flag
		nil,       // arguments
	)
	if err != nil {
		return nil, err
	}

	// Bind the queue to each of the routing keys.
	for _, routingKey := range routingKeys {
		logger.Log.Infof("binding key '%s' in exchange '%s' to queue '%s'", routingKey, exchange, queue.Name)
		err = ch.QueueBind(
			queue.Name, // queue name
			routingKey, // routing key
			exchange,   // exchange name
			false,      // no-wait flag
			nil,        // arguments
		)
		if err != nil {
			return nil, fmt.Errorf("unable to bind %s to the AMQP queue: %s", routingKey, err)
		}
	}

	// Create and return the consumer channel.
	return ch.Consume(
		queue.Name, // queue name
		"",         // consumer name,
		false,      // auto-ack flag
		false,      // exclusive flag
		false,      // no-local flag
		false,      // no-wait flag
		nil,        // args
	)
}

// getRoutingKeys returns a structure that the recorder uses to determine how to process AMQP messages based on
// routing key.
func getRoutingKeys(cfg *viper.Viper) *database.KeyNames {
	routingKeys := cfg.GetStringMapString("dataone.amqp-routing-keys")
	return &database.KeyNames{
		Read: routingKeys["read"],
	}
}

// initService initializes the DataONE indexer service.
func initService() *DataoneIndexer {

	// Parse the command-line options.
	kingpin.Parse()

	// Load the configuration file.
	cfg, err := configurate.InitDefaultsR(*config, configurate.JobServicesDefaults)
	if err != nil {
		logger.Log.Fatalf("unable to load the configuration: %s", err)
	}

	// Establish the database connection.
	db, err := getDbConnection(cfg.GetString("db.uri"))
	if err != nil {
		logger.Log.Fatalf("unable to establish the database connection: %s", err)
	}

	// Create the AMQP channel.
	messages, err := getAmqpChannel(cfg)
	if err != nil {
		logger.Log.Fatalf("unable to subscribe to AMQP messages: %s", err)
	}

	return &DataoneIndexer{
		cfg:      cfg,
		messages: messages,
		db:       db,
		rootDir:  cfg.GetString("dataone.repository-root"),
		recorder: database.NewRecorder(db, getRoutingKeys(cfg), cfg.GetString("dataone.node-id")),
	}
}

// processMessage processes a single AMQP message, returning an error if the message could not be processed.
func (svc *DataoneIndexer) processMessage(delivery amqp.Delivery) error {
	key := delivery.RoutingKey

	// Decode the message body.
	msg, err := model.Decode(delivery.Body)
	if err != nil {
		return fmt.Errorf("unable to parse message (%s): %s", delivery.Body, err)
	}

	// Ignore files that are not in the repository.
	if strings.Index(msg.Path, svc.rootDir) != 0 {
		return nil
	}

	// Record the message.
	if err := svc.recorder.RecordEvent(key, msg); err != nil {
		return fmt.Errorf("unable to record message (%s): %s", delivery.Body, err)
	}

	return nil
}

// processMessages iterates through incoming AMQP messages and records qualifying events.
func (svc *DataoneIndexer) processMessages() {
	for delivery := range svc.messages {
		err := svc.processMessage(delivery)
		if err == nil {
			delivery.Ack(false)
		} else {
			logger.Log.Error(err)
			delivery.Nack(false, false)
		}
	}
}

// main initializes and runs the DataONE indexer service.
func main() {
	svc := initService()

	// Listen for incoming messages forever.
	logger.Log.Info("waiting for incoming AMQP messages")
	spinner := make(chan bool)
	go func() {
		svc.processMessages()
	}()
	<-spinner
}
