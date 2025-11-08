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
	_, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.SimpleQueueTransient)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gs := gamelogic.NewGameState(username)
	// err = pubsub.SubscribeJSON()
	// if err != nil {

	// }

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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	// Handler function that accepts a routing.PlayingState struct.
	// The handler we pass into SubscribeJSON that will be called each time a new message is consumed
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		// pause the game for the client
		gs.HandlePause(ps)
	}
}
