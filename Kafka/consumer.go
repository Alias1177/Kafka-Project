package main

import (
	"awesomeProject/Kafka-Project/Kafka/models"
	"encoding/json"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-chi/chi/v5"
	"log"
	"net/http"
	"time"
)

type Consumer struct {
	consumer *kafka.Consumer
}

func NewConsumer() (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"group.id":          "user-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}

	// Подписываемся на топик
	if err := c.Subscribe("users", nil); err != nil {
		return nil, err
	}

	return &Consumer{consumer: c}, nil
}

func (c *Consumer) Get(w http.ResponseWriter, r *http.Request) {
	messages := make([]models.User, 0)
	timeout := 5 * time.Second

	for {
		msg, err := c.consumer.ReadMessage(timeout)
		if err != nil {
			// Изменяем проверку на более безопасную
			var ke kafka.Error
			if errors.As(err, &ke) && ke.Code() == kafka.ErrTimedOut {
				break
			}
			http.Error(w, "Error reading from Kafka", http.StatusInternalServerError)
			return
		}

		var user models.User
		if err := json.Unmarshal(msg.Value, &user); err != nil {
			continue
		}
		messages = append(messages, user)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(messages)
}

func main() {
	consumer, err := NewConsumer()
	if err != nil {
		log.Fatal("Failed to create consumer:", err)
	}
	defer consumer.consumer.Close()

	r := chi.NewRouter()
	r.Get("/users", consumer.Get)

	log.Println("Consumer server starting on :8081")
	log.Fatal(http.ListenAndServe(":8081", r))
}
