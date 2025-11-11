package main

import (
	"fmt"

	"github.com/taviquenson/pub-sub-learning/internal/gamelogic"
	"github.com/taviquenson/pub-sub-learning/internal/pubsub"
	"github.com/taviquenson/pub-sub-learning/internal/routing"
)

func handlerLogs() func(routing.GameLog) pubsub.Acktype {
	return func(gl routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")

		err := gamelogic.WriteLog(gl)
		if err != nil {
			fmt.Printf("error writing log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
