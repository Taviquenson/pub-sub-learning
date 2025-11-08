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
		log.Fatalf("couldn't get username: %v", err)
	}

	gs := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, routing.PauseKey+"."+gs.GetUsername(), routing.PauseKey, pubsub.SimpleQueueTransient, handlerPause(gs))
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	for { // REPL for the client
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "spawn":
			err = gs.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
			}
		case "move":
			_, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
			}

			// TODO: publish the move
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			// TODO: publish n malicious logs
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command")
		}
	}

}
