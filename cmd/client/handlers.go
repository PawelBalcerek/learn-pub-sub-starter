package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func pauseHandler(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")

		gs.HandlePause(ps)

		return pubsub.Ack
	}
}

func armyMovesHandler(gs *gamelogic.GameState, channel *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		outcome := gs.HandleMove(am)

		switch outcome {
		case gamelogic.MoveOutComeSafe, gamelogic.MoveOutcomeSamePlayer:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			if err := pubsub.PublishJSON(
				channel,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, am.Player.Username),
				gamelogic.RecognitionOfWar{
					Attacker: am.Player,
					Defender: gs.GetPlayerSnap(),
				},
			); err != nil {
				log.Printf("Failed to publish recognition of war: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			log.Println("Unknown move outcome")
			return pubsub.NackDiscard
		}
	}
}

func warHandler(gs *gamelogic.GameState, channel *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(row gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		switch outcome, winner, loser := gs.HandleWar(row); outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
			if err := publishGameLog(
				channel,
				row.Attacker.Username,
				fmt.Sprintf("%s has won a war against %s", winner, loser),
			); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			if err := publishGameLog(
				channel,
				row.Attacker.Username,
				fmt.Sprintf("A war between %s and %s has resulted in a draw", winner, loser),
			); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			log.Printf("Unknown war outcome")
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(ch *amqp.Channel, username, message string) error {
	if err := pubsub.PublishGob(
		ch,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.GameLogSlug, username),
		routing.GameLog{
			CurrentTime: time.Now(),
			Message:     message,
			Username:    username,
		},
	); err != nil {
		log.Printf("Failed to publish game log: %v", err)
		return err
	}
	return nil
}
