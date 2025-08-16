package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alimwb/kafka-go-examle/src/internal/events"
	"github.com/segmentio/kafka-go"
)

func mustEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	broker := mustEnv("KAFKA_BROKER", "localhost:9094")
	topic := mustEnv("KAFKA_TOPIC", "users.signup")
	group := mustEnv("KAFKA_GROUP", "notifier")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        group,
		MinBytes:       1,
		MaxBytes:       10e6,
		SessionTimeout: 10 * time.Second,
		CommitInterval: 0,
	})
	defer r.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		cancel()
	}()

	log.Println("notifier is running...")

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("shutting down...")
				return
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		var ev events.UserSignup
		if err := json.Unmarshal(m.Value, &ev); err != nil {
			log.Printf("bad message (will skip): %v", err)
			// TODO
		} else {
			fmt.Printf("📧 send welcome email to %s (user=%s)\n", ev.Email, ev.UserID)
		}

		if err := r.CommitMessages(ctx, m); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}
