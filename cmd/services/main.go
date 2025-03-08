// main.go
package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"go-kafka-postgres/consumer"
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
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type"},
		AllowCredentials: false,
		MaxAge:           300, // Maximum value not ignored by any of major browsers
	}))

	handler := consumer.NewUser(producer)

	r.Post("/users", handler.Post)
	log.Fatal(http.ListenAndServe(":8080", r))
}
