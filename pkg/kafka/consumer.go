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

func NewKafkaConsumer[T any](brokers []string, groupID string, topic string) (*KafkaConsumer[T], error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar consumer: %w", err)
	}

	return &KafkaConsumer[T]{
		consumerGroup: consumerGroup,
		topic:         topic,
	}, nil
}

func (k *KafkaConsumer[T]) Run(ctx context.Context, msgCh chan T) {
	handler := &ConsumerHandler[T]{msgCh: msgCh}
	for {
		select {
		case <-ctx.Done():
			log.Println("consumer encerrado por contexto")
			return
		default:
			err := k.consumerGroup.Consume(ctx, []string{k.topic}, handler)
			if err != nil {
				log.Printf("erro no consumo: %v", err)
			}
		}
	}
}

func (k *KafkaConsumer[T]) Close() error {
	log.Println("encerrando kafka consumer...")
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
			log.Printf("erro ao desserializar mensagem: %v", err)
			continue
		}
		h.msgCh <- message
		session.MarkMessage(msg, "")
	}
	return nil
}
