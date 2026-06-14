package main

import (
	"context"
	"log"
	"os"
	"os/signal"

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

	<-ctx.Done()

	log.Println("Peril server is shutting down...")
}
