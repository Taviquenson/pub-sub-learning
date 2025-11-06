package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	// Marshal the val to JSON bytes
	jsonData, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("couldn't marshal provided val struct: %v", err)
	}
	ch.PublishWithContext(context.Background(), exchange, key, false, false,
		amqp.Publishing{ContentType: "application/json", Body: jsonData})

	return nil
}

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

// Declare and bind a transient/durable queue
func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	durable := false
	autoDelete := false
	exclusive := false

	switch queueType {
	case Durable:
		durable = true
	case Transient:
		autoDelete = true
		exclusive = true
	default:
		return nil, amqp.Queue{}, fmt.Errorf("only constants Durable or Transient are valid options, received: %v", queueType)
	}

	// Declare a new queue
	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("couldn't declare RabbitMQ queue %v: %v", queueName, err)
	}

	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("couldn't bind RabbitMQ queue %v: %v", queue.Name, err)
	}

	return ch, queue, nil
}
