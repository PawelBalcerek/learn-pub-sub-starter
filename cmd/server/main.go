package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	log.Println("Starting Peril server...")

	log.Println("Connecting to RabbitMQ....")

	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Panicf("Failed to dial RabbitMQ: %v", err)
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Failed to open RabbitMQ channel: %v", err)
	}

	log.Println("Connected to RabbitMQ successfully.")

	if err := pubsub.SubscribeGob(
		connection,
		routing.GameLogSlug,
		pubsub.DurableQueue,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		handleGameLog(),
	); err != nil {
		log.Panicf("Failed to subscribe to game logs: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			log.Println("Sending a pause message")
			if err := sendPausedState(channel, true); err != nil {
				log.Panicf("Failed to publish pause message: %v", err)
			}
		case "resume":
			log.Println("Sending a resume message")
			if err := sendPausedState(channel, false); err != nil {
				log.Panicf("Failed to publish resume message: %v", err)
			}
		case "quit":
			log.Println("Peril server is shutting down...")
			return
		default:
			log.Println("No such command")
		}
	}
}

func sendPausedState(ch *amqp.Channel, isPaused bool) error {
	return pubsub.PublishJSON(
		ch,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{IsPaused: isPaused},
	)
}

func handleGameLog() func(routing.GameLog) pubsub.AckType {
	return func(gl routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")

		gamelogic.WriteLog(gl)

		return pubsub.Ack
	}
}
