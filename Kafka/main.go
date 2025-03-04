// main.go
package main

import (
	"awesomeProject/Kafka-Project/Kafka/models"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-chi/chi/v5"
	"log"
	"net/http"
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
	handler := models.NewUser(producer)

	r.Post("/users", handler.Post)
	log.Fatal(http.ListenAndServe(":8080", r))
}
