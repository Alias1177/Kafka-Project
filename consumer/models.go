package consumer

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"net/http"
)

type User struct {
	Name string          `json:"name"`
	Age  int             `json:"age"`
	k    *kafka.Producer `json:"-"`
}

func NewUser(producer *kafka.Producer) *User {
	return &User{
		k: producer,
	}
}

func (u *User) Post(w http.ResponseWriter, r *http.Request) {
	// Используем временную структуру для декодирования только нужных полей
	var input struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Обновляем поля текущего пользователя
	u.Name = input.Name
	u.Age = input.Age

	// Сериализуем данные для отправки в Kafka
	message, err := json.Marshal(input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	topic := "users"
	err = u.k.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: message,
	}, nil)

	if err != nil {
		http.Error(w, "Error sending message to Kafka: "+err.Error(), http.StatusInternalServerError)
		return
	}

	u.k.Flush(15 * 1000)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Message sent to Kafka successfully"))
}
