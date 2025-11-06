package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/taviquenson/pub-sub-learning/internal/pubsub"
	"github.com/taviquenson/pub-sub-learning/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	// this is how app knows where to connect to the RabbitMQ server
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("couldn't connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ")

	// Create RabbitMQ Channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("couldn't create RabbitMQ channel: %v", err)
	}

	// Publish a message to the exchange
	pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})

	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("RabbitMQ connection closed")
}
