package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/taviquenson/pub-sub-learning/internal/gamelogic"
	"github.com/taviquenson/pub-sub-learning/internal/pubsub"
	"github.com/taviquenson/pub-sub-learning/internal/routing"
)

func main() {
	fmt.Println("Starting Peril client...")

	// this is how app knows where to connect to the RabbitMQ server
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("client couldn't connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ")

	// Prompt user for their username
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("couldn't create username: %v", err)
	}

	// Declare and bind a channel for the username on the exchange peril_direct
	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatalf("client couldn't declare and bind a RabbitMQ channel: %v", err)
	}

	gs := gamelogic.NewGameState(username)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "spawn":
			err = gs.CommandSpawn(words)
			if err != nil {
				fmt.Println(err.Error())
			}
		case "move":
			_, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err.Error())
			}
			fmt.Printf("Successfully moved player %s's units", username)
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command")
		}
	}

	// wait for ctrl+c
	// signalChan := make(chan os.Signal, 1)
	// signal.Notify(signalChan, os.Interrupt)
	// <-signalChan
	// fmt.Println("RabbitMQ client connection closed")

}
