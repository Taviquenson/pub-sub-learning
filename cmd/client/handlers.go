package main

import (
	"fmt"

	"github.com/taviquenson/pub-sub-learning/internal/gamelogic"
	"github.com/taviquenson/pub-sub-learning/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	// Handler function that accepts a routing.PlayingState struct.
	// The handler we pass into SubscribeJSON that will be called each time a new message is consumed
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		// pause the game for the client
		gs.HandlePause(ps)
	}
}
