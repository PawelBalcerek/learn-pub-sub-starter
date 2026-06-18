package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.Println("Starting Peril server...")

	log.Println("Connecting to RabbitMQ....")

	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Panicf("Failed to dial RabbitMQ: %v", err)
	}
	defer connection.Close()

	log.Println("Connected to RabbitMQ successfully.")

	channel, err := connection.Channel()
	if err != nil {
		log.Panicf("Failed to create a RabbitMQ channel: %v", err)
	}
	if err := pubsub.PublishJSON(
		channel,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{
			IsPaused: true,
		},
	); err != nil {
		log.Panicf("Failed to publish pause msg: %v", err)
	}

	<-ctx.Done()

	log.Println("Peril server is shutting down...")
}
