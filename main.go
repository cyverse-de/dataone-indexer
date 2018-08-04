// dataone-indexer
//
// This service listens to the AMQP exchange for the CyVerse data store for events that are relevant to the DataONE
// member node service and records them in the DataONE event database.
package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

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
	config    = kingpin.Flag("config", "Path to configuration file.").Short('c').Required().File()
	intervals = []int{500, 1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000, 256000, 256000}
)

// DataoneIndexer represents this service.
type DataoneIndexer struct {
	cfg      *viper.Viper
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

// getAmqpConnection establishes a connection to the AMQP broker.
func getAmqpConnection(uri string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(uri)
	if err == nil {
		return conn, nil
	}

	// The first attempt failed. Keep trying until we connect or reach the maximum number of retries.
	for _, ms := range intervals {
		delay := ms/2 + rand.Intn(ms)
		logger.Log.Errorf("failed to connect to AMQP: %s", err)
		logger.Log.Infof("trying to connect again in %d milliseconds", delay)
		time.Sleep(time.Duration(delay) * time.Millisecond)
		conn, err = amqp.Dial(uri)
		if err == nil {
			return conn, nil
		}
	}

	// We've reached the maximum number of connection attempts.
	logger.Log.Error("failed to connect after maximum number of attempts - giving up")
	return nil, err
}

// closeAmqpConnection closes an AMQP connection and logs a warning if it can't be closed.
func closeAmqpConnection(conn *amqp.Connection) {
	err := conn.Close()
	if err != nil {
		logger.Log.Warnf("failed to close the AMQP connection: %s", err)
	}
}

// getMsgChannel establishes a connection to the AMQP Broker and returns a channel to use for receiving messages.
func getMsgChannel(cfg *viper.Viper) (*amqp.Connection, <-chan amqp.Delivery, error) {
	uri := cfg.GetString("amqp.uri")
	exchange := cfg.GetString("amqp.exchange.name")
	queueName := "dataone.events"
	routingKeys := cfg.GetStringMapString("dataone.amqp-routing-keys")

	// Establish the AMQP connection.
	conn, err := getAmqpConnection(uri)
	if err != nil {
		return nil, nil, err
	}

	// Create the AMQP channel.
	ch, err := conn.Channel()
	if err != nil {
		closeAmqpConnection(conn)
		return nil, nil, err
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
		closeAmqpConnection(conn)
		return nil, nil, err
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
			closeAmqpConnection(conn)
			return nil, nil, fmt.Errorf("unable to bind %s to the AMQP queue: %s", routingKey, err)
		}
	}

	// Create and return the consumer channel.
	messages, err := ch.Consume(
		queue.Name, // queue name
		"",         // consumer name,
		false,      // auto-ack flag
		false,      // exclusive flag
		false,      // no-local flag
		false,      // no-wait flag
		nil,        // args
	)
	if err != nil {
		closeAmqpConnection(conn)
		return nil, nil, fmt.Errorf("unable to consume AMQP messages: %s", err)
	}

	return conn, messages, nil
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
	cfg, err := configurate.InitDefaultsR(*config, defaultConfig)
	if err != nil {
		logger.Log.Fatalf("unable to load the configuration: %s", err)
	}

	// Establish the database connection.
	db, err := getDbConnection(cfg.GetString("db.uri"))
	if err != nil {
		logger.Log.Fatalf("unable to establish the database connection: %s", err)
	}

	return &DataoneIndexer{
		cfg:      cfg,
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

	// Initialize the AMQP connection.
	conn, ch, err := getMsgChannel(svc.cfg)
	if err != nil {
		logger.Log.Fatalf("failed to initialize the AMQP connection: %s", err)
	}

	// Create a channel for lost connection notifications.
	notifyClose := conn.NotifyClose(make(chan *amqp.Error))

	for {
		select {
		case closeError := <-notifyClose:
			logger.Log.Error("connection lost: %s", closeError)
			conn, ch, err = getMsgChannel(svc.cfg)
			if err != nil {
				logger.Log.Fatalf("failed to restore the AMQP connection: %s", err)
			}
			notifyClose = conn.NotifyClose(make(chan *amqp.Error))

		case delivery := <-ch:
			err = svc.processMessage(delivery)
			if err == nil {
				err = delivery.Ack(false)
				if err != nil {
					logger.Log.Warnf("unable to acknowledge AMQP message: %s", err)
				}
			} else {
				logger.Log.Errorf("failed to process message: %s", err)
				err = delivery.Nack(false, false)
				if err != nil {
					logger.Log.Warnf("unable to negatively acknowledge AMQP message: %s", err)
				}
			}
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
