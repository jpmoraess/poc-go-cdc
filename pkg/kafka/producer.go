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

func NewKafkaProducer[T any](opts ...func(*KafkaProducerConfig)) (*KafkaProducer[T], error) {
	config := DefaultKafkaProducerConfig()
	for _, opt := range opts {
		opt(config)
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.Return.Successes = config.ProducerReturnSuccesses
	saramaConfig.Producer.Retry.Max = config.ProducerRetryMax
	saramaConfig.Producer.RequiredAcks = config.ProducerRequiredAcks

	client, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar kafka producer: %w", err)
	}

	return &KafkaProducer[T]{
		client: client,
		topic:  config.Topic,
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

// CONFIGURATION

type KafkaProducerConfig struct {
	Brokers                 []string
	Topic                   string
	ProducerReturnSuccesses bool
	ProducerRetryMax        int
	ProducerRequiredAcks    sarama.RequiredAcks
}

func DefaultKafkaProducerConfig() *KafkaProducerConfig {
	return &KafkaProducerConfig{
		ProducerReturnSuccesses: true,
		ProducerRetryMax:        5,
		ProducerRequiredAcks:    sarama.WaitForAll,
	}
}

func WithBrokers(brokers []string) func(*KafkaProducerConfig) {
	return func(c *KafkaProducerConfig) {
		c.Brokers = brokers
	}
}

func WithTopic(topic string) func(*KafkaProducerConfig) {
	return func(c *KafkaProducerConfig) {
		c.Topic = topic
	}
}

func WithProducerReturnSuccesses(returnSuccesses bool) func(*KafkaProducerConfig) {
	return func(c *KafkaProducerConfig) {
		c.ProducerReturnSuccesses = returnSuccesses
	}
}

func WithProducerRetryMax(retryMax int) func(*KafkaProducerConfig) {
	return func(c *KafkaProducerConfig) {
		c.ProducerRetryMax = retryMax
	}
}

func WithProducerRequiredAcks(requiredAcks sarama.RequiredAcks) func(*KafkaProducerConfig) {
	return func(c *KafkaProducerConfig) {
		c.ProducerRequiredAcks = requiredAcks
	}
}
