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
		armyMovesHandler(gameState),
	); err != nil {
		log.Fatalf("Failed to declare and bind %s queue: %v", armyMovesQueue, err)
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
			}
		case "move":
			armyMove, err := gameState.CommandMove(words)
			if err != nil {
				log.Printf("Move command has failed: %v", err)
			}
			if err := pubsub.PublishJSON(
				channel,
				routing.ExchangePerilTopic,
				armyMovesKey,
				armyMove,
			); err != nil {
				log.Printf("Failed to publish army move: %v", err)
			}
			log.Printf("Successfuly published army move.")
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

func pauseHandler(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")

		gs.HandlePause(ps)

		return pubsub.Ack
	}
}

func armyMovesHandler(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		outcome := gs.HandleMove(am)

		switch outcome {
		case gamelogic.MoveOutComeSafe, gamelogic.MoveOutcomeMakeWar:
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			fallthrough
		default:
			return pubsub.NackDiscard
		}
	}
}
