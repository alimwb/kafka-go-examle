package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
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

	w := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 20 * time.Millisecond,
		Compression:  kafka.Snappy,
	}
	defer w.Close()

	ev := events.UserSignup{
		Event:   "user_registered",
		Version: 1,
		UserID:  "u-124",
		Email:   "alice@example.com",
		TS:      time.Now().UTC(),
	}

	payload, err := json.Marshal(ev)
	if err != nil {
		log.Fatal(err)
	}

	msg := kafka.Message{
		Key:   []byte(ev.UserID),
		Value: payload,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "type", Value: []byte("UserSignup")},
			{Key: "version", Value: []byte("1")},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.WriteMessages(ctx, msg); err != nil {
		log.Fatalf("write failed: %v", err)
	}

	fmt.Println("✔ sent:", string(payload))
}
