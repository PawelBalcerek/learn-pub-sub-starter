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

	gameState := gamelogic.NewGameState(username)

	if err := pubsub.SubscribeJSON(
		connection,
		fmt.Sprintf("pause.%s", username),
		pubsub.TransientQueue,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		pauseHandler(gameState),
	); err != nil {
		log.Panicf("Failed to declare and bind queue: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			if err := gameState.CommandSpawn(words); err != nil {
				log.Printf("spawn command failed: %v", err)
			}
		case "move":
			if _, err := gameState.CommandMove(words); err != nil {
				log.Printf("move command failed: %v", err)
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			log.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			log.Println("No such command")
		}
	}
}

func pauseHandler(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")

		gs.HandlePause(ps)
	}
}
