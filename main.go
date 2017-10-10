// dataone-indexer
//
// This service listens to the AMQP exchange for the CyVerse data store for events that are relevant to the DataONE
// member node service and records them in the DataONE event database.
package main

import (
	"database/sql"
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
    durable: true
    auto-delete: false
  routing-key:
    subscription: data-object.*
    read: data-object.open

db:
  uri: postgresql://guest:guest@dedb:5432/de?sslmode=disable

repository:
  root: /iplant/home/shared/commons_repo/curated
`

// Command-line option definitions.
var (
	config = kingpin.Flag("config", "Path to configuration file.").Short('c').Required().File()
)

type Messenger interface {
}

type DataoneIndexer struct {
	cfg      *viper.Viper
	messages <-chan amqp.Delivery
	db       *sql.DB
	rootDir  string
}

// getDbConnection establishes a connection to the DE database.
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
	durable := cfg.GetBool("amqp.exchange.durable")
	autoDelete := cfg.GetBool("amqp.exchange.auto-delete")
	queueName := "dataone.events"
	routingKey := cfg.GetString("amqp.routing-key.subscription")

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

	// Declare the exchange.
	err = ch.ExchangeDeclare(
		exchange,   // exchange name
		"topic",    // exchange type
		durable,    // exchnage durable
		autoDelete, // exchange auto-delete flag
		false,      // exchange internal flag
		false,      // exchange no-wait flag
		nil,        // arguments
	)
	if err != nil {
		return nil, err
	}

	// Declare the queue.
	queue, err := ch.QueueDeclare(
		queueName, // queue name
		false,     // queue durable
		false,     // queue auto-delete flag
		false,     // queue exclusive flag
		false,     // queue no-wait flag
		nil,       // arguments
	)
	if err != nil {
		return nil, err
	}

	// Bind the queue to the routing key.
	err = ch.QueueBind(
		queue.Name, // queue name
		routingKey, // routing key
		exchange,   // exchange name
		false,      // no-wait flag
		nil,        // arguments
	)
	if err != nil {
		return nil, err
	}

	// Create and return the consumer channel.
	return ch.Consume(
		queue.Name, // queue name
		"",         // consumer name,
		true,       // auto-ack flag
		false,      // exclusive flag
		false,      // no-local flag
		false,      // no-wait flag
		nil,        // args
	)
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

	// Create the AMQP channel.
	messages, err := getAmqpChannel(cfg)
	if err != nil {
		logger.Log.Fatalf("Unable to subscribe to AMQP messages: %s", err)
	}

	return &DataoneIndexer{
		cfg:      cfg,
		messages: messages,
		db:       db,
		rootDir:  cfg.GetString("repository.root"),
	}
}

func (svc *DataoneIndexer) processMessages() {
	for delivery := range svc.messages {
		key := delivery.RoutingKey
		msg, err := model.Decode(delivery.Body)
		if err != nil {
			logger.Log.Errorf("Unable to parse message (%s): %s", delivery.Body, err)
		}
		if strings.Index(msg.Path, svc.rootDir) == 0 {
			if err := database.RecordMessage(key, msg); err != nil {
				logger.Log.Errorf("Unable to record message (%s): %s", delivery.Body, err)
			}
		}
	}
}

func main() {
	svc := initService()

	// Listen for incoming messages forever.
	spinner := make(chan bool)
	go func() {
		svc.processMessages()
	}()
	<-spinner
}
