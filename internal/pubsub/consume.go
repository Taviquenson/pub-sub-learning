package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// type Acktype int

type SimpleQueueType int

const ( // here the constants are assigned ints. First 0, then 1
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	// Make sure that the given queue exists and is bound to the exchange,
	// if it isn't, this will create said queue
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("error in SubscribeJSON(): %v", err)
	}

	// Getting a new chan of amqp.Delivery structs (queued messages)
	// Using an empty string for the consumer name so that it will be auto-generated
	msgsCh, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error trying to consume messages: %v", err)
	}

	go func() {
		for msg := range msgsCh {
			var dataStruc T // a Go structure for data from JSON in queue msgs
			if err = json.Unmarshal(msg.Body, &dataStruc); err != nil {
				fmt.Printf("error unmarshalling JSON: %v", err)
			}
			handler(dataStruc)
			msg.Ack(false) // Acknowledge the message to remove it from the queue
			if err != nil {
				fmt.Printf("error: the acknowledge could not be delivered to the channel it was sent from")
			}
		}
	}()

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err)
	}

	queue, err := ch.QueueDeclare(
		queueName,                       // name
		queueType == SimpleQueueDurable, // durable
		queueType != SimpleQueueDurable, // delete when unused
		queueType != SimpleQueueDurable, // exclusive
		false,                           // no-wait
		nil,                             // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err)
	}

	err = ch.QueueBind(
		queue.Name, // queue name
		key,        // routing key
		exchange,   // exchange
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err)
	}
	return ch, queue, nil
}
