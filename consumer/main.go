package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal("Failed to create a new consumer: ", err.Error())
	}
	defer consumer.Close()

	partitionKafkaTry, err := consumer.ConsumePartition("kafka-try", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("Failed to get a new partition: ", err.Error())
	}
	defer partitionKafkaTry.Close()

	for {
		select {
		case msg := <-partitionKafkaTry.Messages():
			fmt.Println("received message: ", string(msg.Value))
		case err := <-partitionKafkaTry.Errors():
			fmt.Println(err.Error())
		}

	}
}
