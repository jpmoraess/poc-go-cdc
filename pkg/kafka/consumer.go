package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type KafkaConsumer[T any] struct {
	consumerGroup sarama.ConsumerGroup
	topic         string
}

func NewKafkaConsumer[T any](opts ...func(*KafkaConsumerConfig)) (*KafkaConsumer[T], error) {
	config := DefaultKafkaConsumerConfig()
	for _, opt := range opts {
		opt(config)
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Group.Rebalance.Strategy = config.ConsumerRebalance
	saramaConfig.Consumer.Offsets.Initial = config.OffsetsInitial

	consumerGroup, err := sarama.NewConsumerGroup(config.Brokers, config.ConsumerGroup, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating consumer: %w", err)
	}

	return &KafkaConsumer[T]{
		consumerGroup: consumerGroup,
		topic:         config.Topic,
	}, nil
}

func (k *KafkaConsumer[T]) Run(ctx context.Context, msgCh chan T) {
	handler := &ConsumerHandler[T]{msgCh: msgCh}
	for {
		select {
		case <-ctx.Done():
			log.Println("consumer terminated by context")
			return
		default:
			err := k.consumerGroup.Consume(ctx, []string{k.topic}, handler)
			if err != nil {
				log.Printf("error consuming: %v", err)
			}
		}
	}
}

func (k *KafkaConsumer[T]) Close() error {
	log.Println("closing Kafka consumer...")
	return k.consumerGroup.Close()
}

type ConsumerHandler[T any] struct {
	msgCh chan T
}

func (h *ConsumerHandler[T]) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerHandler[T]) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerHandler[T]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var message T
		if err := json.Unmarshal(msg.Value, &message); err != nil {
			log.Printf("error deserializing message: %v", err)
			continue
		}
		h.msgCh <- message
		session.MarkMessage(msg, "")
	}
	return nil
}

// CONFIGURATION

type KafkaConsumerConfig struct {
	Brokers           []string
	Topic             string
	ConsumerGroup     string
	ConsumerRebalance sarama.BalanceStrategy
	OffsetsInitial    int64
}

func DefaultKafkaConsumerConfig() *KafkaConsumerConfig {
	return &KafkaConsumerConfig{
		ConsumerRebalance: sarama.NewBalanceStrategyRoundRobin(),
		OffsetsInitial:    sarama.OffsetNewest,
	}
}

func WithConsumerBrokers(brokers []string) func(*KafkaConsumerConfig) {
	return func(c *KafkaConsumerConfig) {
		c.Brokers = brokers
	}
}

func WithConsumerGroup(group string) func(*KafkaConsumerConfig) {
	return func(c *KafkaConsumerConfig) {
		c.ConsumerGroup = group
	}
}

func WithConsumerTopic(topic string) func(*KafkaConsumerConfig) {
	return func(c *KafkaConsumerConfig) {
		c.Topic = topic
	}
}

func WithConsumerRebalance(rebalance sarama.BalanceStrategy) func(*KafkaConsumerConfig) {
	return func(c *KafkaConsumerConfig) {
		c.ConsumerRebalance = rebalance
	}
}

func WithOffsetsInitial(offset int64) func(*KafkaConsumerConfig) {
	return func(c *KafkaConsumerConfig) {
		c.OffsetsInitial = offset
	}
}
