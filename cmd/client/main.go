package main

import (
	"fmt"
	"log"
	"strconv"

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
		log.Fatalf("Failed to dial RabbitMQ: %v", err)
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Failed to open RabbitMQ channel: %v", err)
	}

	log.Println("Connected to RabbitMQ successfully.")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to obtain username: %v", err)
	}

	gameState := gamelogic.NewGameState(username)

	pauseQueue := fmt.Sprintf("pause.%s", username)
	if err := pubsub.SubscribeJSON(
		connection,
		pauseQueue,
		pubsub.TransientQueue,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		pauseHandler(gameState),
	); err != nil {
		log.Fatalf("Failed to declare and bind %s queue: %v", pauseQueue, err)
	}

	armyMovesQueue := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
	armyMovesKey := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	if err := pubsub.SubscribeJSON(
		connection,
		armyMovesQueue,
		pubsub.TransientQueue,
		routing.ExchangePerilTopic,
		armyMovesKey,
		armyMovesHandler(gameState, channel),
	); err != nil {
		log.Fatalf("Failed to declare and bind %s queue: %v", armyMovesQueue, err)
	}

	warQueue := routing.WarRecognitionsPrefix
	warKey := fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
	if err := pubsub.SubscribeJSON(
		connection,
		warQueue,
		pubsub.DurableQueue,
		routing.ExchangePerilTopic,
		warKey,
		warHandler(gameState, channel),
	); err != nil {
		log.Fatalf("Failed to declar and bind %s, queue: %v", warQueue, err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			if err := gameState.CommandSpawn(words); err != nil {
				log.Printf("Spawn command has failed: %v", err)
				continue
			}
		case "move":
			armyMove, err := gameState.CommandMove(words)
			if err != nil {
				log.Printf("Move command has failed: %v", err)
				continue
			}
			if err := pubsub.PublishJSON(
				channel,
				routing.ExchangePerilTopic,
				armyMovesKey,
				armyMove,
			); err != nil {
				log.Printf("Failed to publish army move: %v", err)
				continue
			}
			log.Printf("Successfuly published army move.")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) < 2 {
				log.Printf("Invalid spam command usage, it should be: spam <count>")
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				log.Printf("Failed to parse count: %v", err)
				continue
			}
			for range n {
				publishGameLog(channel, username, gamelogic.GetMaliciousLog())
			}
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			log.Println("No such command")
		}
	}
}
