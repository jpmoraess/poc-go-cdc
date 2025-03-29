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
	producer, err := kafka.NewKafkaProducer[MyMessage](brokers, topic)
	if err != nil {
		log.Fatalf("erro ao inicializar o producer: %v", err)
	}
	defer producer.Close()

	go sendMessage(ctx, producer)

	// consumer
	consumer, err := kafka.NewKafkaConsumer[MyMessage](brokers, groupID, topic)
	if err != nil {
		log.Fatalf("erro ao iniciar o consumer: %v", err)
	}
	defer consumer.Close()

	msgCh := make(chan MyMessage)

	go consumer.Run(ctx, msgCh)
	go consumer.Run(ctx, msgCh)

	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				log.Println("canal fechado, encerrando consumo")
				return
			}
			fmt.Printf("mensagem recebida: %+v\n", msg)
		case <-ctx.Done():
			log.Println("contexto cancelado, encerrando consumo")
			return
		}
	}
}

func sendMessage(ctx context.Context, producer *kafka.KafkaProducer[MyMessage]) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("producer encerrado...")
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
				log.Printf("erro ao enviar mensagem: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
