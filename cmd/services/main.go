// main.go
package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go-kafka-postgres/Kafka-Project/consumer"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"client.id":         "order-producer",
		"acks":              "all",
	})
	if err != nil {
		log.Fatal("Failed to create producer:", err)
	}
	defer producer.Close()

	r := chi.NewRouter()
	handler := consumer.NewUser(producer)

	r.Post("/users", handler.Post)
	log.Fatal(http.ListenAndServe(":8080", r))
}
