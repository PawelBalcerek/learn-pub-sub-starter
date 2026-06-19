package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Transient SimpleQueueType = iota
	Durable
)

func DeclareAndBindQueue(
	connection *amqp.Connection,
	queueName string,
	queueType SimpleQueueType,
	exchange,
	key string,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := connection.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("failed to open channel: %w", err)
	}
	queue, err := channel.QueueDeclare(
		queueName,
		queueType.isDurable(),
		queueType.autoDelete(),
		queueType.exclusive(),
		false,
		nil,
	)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("failed to declare queue: %w", err)
	}
	if err := channel.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return &amqp.Channel{}, amqp.Queue{}, fmt.Errorf("failed to bind queue to the exchange: %w", err)
	}
	return channel, queue, nil
}

func (s SimpleQueueType) isDurable() bool {
	switch s {
	case Durable:
		return true
	case Transient:
		return false
	default:
		return false
	}
}

func (s SimpleQueueType) autoDelete() bool {
	switch s {
	case Durable:
		return false
	case Transient:
		return true
	default:
		return false
	}
}

func (s SimpleQueueType) exclusive() bool {
	switch s {
	case Durable:
		return false
	case Transient:
		return true
	default:
		return false
	}
}
