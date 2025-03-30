package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/jpmoraess/poc-go-cdc/pkg/kafka"
)

type Receiver struct {
	Name string
}

type MyMessage struct {
	NumberOne int
	NumberTwo int
	Receiver  Receiver
}

type OrderMessage struct {
	OrderID    uuid.UUID
	CustomerID uuid.UUID
	Total      int
}

func main() {
	var (
		brokers = []string{"localhost:19092", "localhost:29092", "localhost:39092"}
		topic   = "debezium.order.payment_outbox"
		groupID = "my-consumer"
	)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		cancel()
	}()

	// producer
	producer, err := kafka.NewKafkaProducer[MyMessage](
		kafka.WithProducerBrokers(brokers),
		kafka.WithProducerTopic(topic),
		kafka.WithProducerReturnSuccesses(true),
		kafka.WithProducerRetryMax(5),
		kafka.WithProducerRequiredAcks(sarama.WaitForAll),
	)
	if err != nil {
		log.Fatalf("error initializing the producer: %v", err)
	}
	defer producer.Close()

	// order producer
	orderProducer, err := kafka.NewKafkaProducer[OrderMessage](
		kafka.WithProducerBrokers(brokers),
		kafka.WithProducerTopic(topic),
		kafka.WithProducerReturnSuccesses(true),
		kafka.WithProducerRetryMax(5),
		kafka.WithProducerRequiredAcks(sarama.WaitForAll),
	)
	if err != nil {
		log.Fatalf("error initializing the order producer: %v", err)
	}
	defer orderProducer.Close()

	go sendOrderMessage(ctx, orderProducer)
	//go sendMessage(ctx, producer)

	// consumer
	consumer, err := kafka.NewKafkaConsumer[MyMessage](
		kafka.WithConsumerBrokers(brokers),
		kafka.WithConsumerTopic(topic),
		kafka.WithConsumerGroup(groupID),
		kafka.WithConsumerRebalance(sarama.NewBalanceStrategyRoundRobin()),
		kafka.WithOffsetsInitial(sarama.OffsetNewest),
	)
	if err != nil {
		log.Fatalf("error initializing the consumer: %v", err)
	}
	defer consumer.Close()

	// order consumer
	orderConsumer, err := kafka.NewKafkaConsumer[OrderMessage](
		kafka.WithConsumerBrokers(brokers),
		kafka.WithConsumerTopic(topic),
		kafka.WithConsumerGroup(groupID),
		kafka.WithConsumerRebalance(sarama.NewBalanceStrategyRoundRobin()),
		kafka.WithOffsetsInitial(sarama.OffsetNewest),
	)
	if err != nil {
		log.Fatalf("error initializing the order consumer: %v", err)
	}
	defer orderConsumer.Close()

	//msgCh := make(chan MyMessage)
	orderMsgCh := make(chan OrderMessage)

	go orderConsumer.Run(ctx, orderMsgCh)

	//go consumer.Run(ctx, msgCh)
	//go consumer.Run(ctx, msgCh)

	for {
		select {
		case msg, ok := <-orderMsgCh:
			if !ok {
				log.Println("channel closed, stopping consumption")
				return
			}
			fmt.Printf("message received: %+v\n", msg)
		case <-ctx.Done():
			log.Println("context canceled, stopping consumption")
			return
		}
	}
}

func sendMessage(ctx context.Context, producer *kafka.KafkaProducer[MyMessage]) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("producer terminated...")
			return
		default:
			rand.Seed(time.Now().UnixNano())
			rand1 := rand.Intn(200)
			rand2 := rand.Intn(350)
			message := MyMessage{
				NumberOne: rand1,
				NumberTwo: rand2,
				Receiver: Receiver{
					Name: "John",
				},
			}
			err := producer.SendMessage(ctx, message)
			if err != nil {
				log.Printf("error while sending message: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func sendOrderMessage(ctx context.Context, producer *kafka.KafkaProducer[OrderMessage]) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("producer terminated...")
			return
		default:
			rand.Seed(time.Now().UnixNano())
			rand1 := rand.Intn(200)
			rand2 := rand.Intn(350)
			message := OrderMessage{
				OrderID:    uuid.New(),
				CustomerID: uuid.New(),
				Total:      rand1 + rand2,
			}
			err := producer.SendMessage(ctx, message)
			if err != nil {
				log.Printf("error while sending message: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
