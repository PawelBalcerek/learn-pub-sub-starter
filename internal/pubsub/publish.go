package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("Failed to marshal val: %w", err)
	}

	if err := publish(ch, exchange, key, "application/json", data); err != nil {
		return err
	}

	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(val); err != nil {
		return fmt.Errorf("Failed to encode val: %w", err)
	}

	if err := publish(ch, exchange, key, "application/gob", buffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func publish(ch *amqp.Channel, exchange, key, contentType string, data []byte) error {
	const (
		optional = false
		delayed  = false
	)
	if err := ch.PublishWithContext(
		context.TODO(),
		exchange,
		key,
		optional,
		delayed,
		amqp.Publishing{
			ContentType: contentType,
			Body:        data,
		},
	); err != nil {
		return fmt.Errorf("Failed to publish data: %w", err)
	}
	return nil
}
