package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal("Failed to create a new producer: ", err.Error())
	}
	defer producer.Close()

	for {
		msg := &sarama.ProducerMessage{
			Topic: "kafka-try",
			Value: sarama.StringEncoder("message" + time.Now().GoString()),
		}

		time.Sleep(time.Second * 3)
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Fatal("Failed to to send message: ", err.Error())
		}

		fmt.Printf("partition: %v, offset: %v", partition, offset)

	}

}
