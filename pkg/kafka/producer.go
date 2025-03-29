package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type KafkaProducer[T any] struct {
	client sarama.SyncProducer
	topic  string
}

func NewKafkaProducer[T any](brokers []string, topic string) (*KafkaProducer[T], error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll

	client, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar kafka producer: %w", err)
	}

	return &KafkaProducer[T]{
		client: client,
		topic:  topic,
	}, nil
}

func (kafka *KafkaProducer[T]) SendMessage(ctx context.Context, message T) error {
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("erro ao serializar mensagem: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: kafka.topic,
		Value: sarama.ByteEncoder(data),
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("contexto cancelado antes do envio da mensagem")
	default:
		partition, offset, err := kafka.client.SendMessage(msg)
		if err != nil {
			return fmt.Errorf("erro ao enviar mensagem: %w", err)
		}
		log.Printf("mensagem: (%s) enviada para (partition: %d, offset: %d)", string(data), partition, offset)
		return nil
	}
}

func (kafka *KafkaProducer[T]) Close() error {
	log.Println("encerrando o kafka producer")
	return kafka.client.Close()
}
