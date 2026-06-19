package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.Println("Starting Peril client...")

	log.Println("Connecting to RabbitMQ....")

	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Panicf("Failed to dial RabbitMQ: %v", err)
	}
	defer connection.Close()

	log.Println("Connected to RabbitMQ successfully.")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Panicf("Failed to obtain username: %v", err)
	}

	if _, _, err := pubsub.DeclareAndBindQueue(
		connection,
		fmt.Sprintf("pause.%s", username),
		pubsub.Transient,
		"peril_direct",
		routing.PauseKey,
	); err != nil {
		log.Panicf("Failed to declare and bind queue: %v", err)
	}

	<-ctx.Done()

	log.Println("Peril client is shutting down...")
}
