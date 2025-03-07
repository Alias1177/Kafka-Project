package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-chi/chi/v5"
	database "go-kafka-postgres/cmd/services/connect"
	"go-kafka-postgres/consumer"
	"log"
	"net/http"
	"time"
)

type Consumer struct {
	consumer *kafka.Consumer
	db       *sql.DB
}

func NewConsumer(db *sql.DB) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"group.id":          "user-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}

	if err := c.Subscribe("users", nil); err != nil {
		return nil, err
	}

	return &Consumer{
		consumer: c,
		db:       db,
	}, nil
}

func (c *Consumer) saveToDatabase(user consumer.User) error {
	query := "INSERT INTO myusers (name, age) VALUES ($1, $2)"
	_, err := c.db.Exec(query, user.Name, user.Age)
	return err
}

func (c *Consumer) GetFromDatabase(w http.ResponseWriter, r *http.Request) {
	rows, err := c.db.Query("SELECT name, age FROM myusers")
	if err != nil {
		http.Error(w, "Error querying database", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var users []consumer.User
	for rows.Next() {
		var user consumer.User
		if err := rows.Scan(&user.Name, &user.Age); err != nil {
			http.Error(w, "Error scanning row", http.StatusInternalServerError)
			return
		}
		users = append(users, user)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(users)
}

func (c *Consumer) GetFromKafka(w http.ResponseWriter, r *http.Request) {
	messages := make([]consumer.User, 0)
	timeout := 5 * time.Second

	err := c.consumer.Seek(kafka.TopicPartition{
		Topic:     &[]string{"users"}[0],
		Partition: 0,
		Offset:    kafka.OffsetBeginning,
	}, 0)
	if err != nil {
		http.Error(w, "Error seeking Kafka topic", http.StatusInternalServerError)
		return
	}

	for {
		msg, err := c.consumer.ReadMessage(timeout)
		if err != nil {
			var ke kafka.Error
			if errors.As(err, &ke) && ke.Code() == kafka.ErrTimedOut {
				break
			}
			http.Error(w, "Error reading from Kafka", http.StatusInternalServerError)
			return
		}

		var user consumer.User
		if err := json.Unmarshal(msg.Value, &user); err != nil {
			log.Printf("Error unmarshaling message: %v", err)
			continue
		}

		messages = append(messages, user)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(messages); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
}

func (c *Consumer) StartProcessing() {
	for {
		msg, err := c.consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		var user consumer.User
		if err := json.Unmarshal(msg.Value, &user); err != nil {
			log.Printf("Error unmarshaling message: %v", err)
			continue
		}

		if err := c.saveToDatabase(user); err != nil {
			log.Printf("Error saving to database: %v", err)
			continue
		}

		log.Printf("Saved user to database: %s", user.Name)
	}
}

func main() {
	db := database.DBConnection()
	defer db.Close()

	consumer, err := NewConsumer(db)
	if err != nil {
		log.Fatal("Failed to create consumer:", err)
	}
	defer consumer.consumer.Close()

	go consumer.StartProcessing()

	r := chi.NewRouter()
	r.Get("/kafka", consumer.GetFromKafka)    // Новый endpoint для чтения из Kafka
	r.Get("/users", consumer.GetFromDatabase) // Существующий endpoint для чтения из БД

	log.Println("Consumer server starting on :8081")
	log.Fatal(http.ListenAndServe(":8081", r))
}
