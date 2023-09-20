package main

import (
    "github.com/confluentinc/confluent-kafka-go/kafka"
    "log"
)

func main() {
    deliveryChan := make(chan kafka.Event)
    producer := NewKafkaProducer()
    Publish("Transferencia", "teste", producer, []byte("transferencia"), deliveryChan)

    e := <-deliveryChan
    msg := e.(*kafka.Message)

    if msg.TopicPartition.Error != nil {
        log.Println("Erro ao enviar!")
    } else {
        log.Println("Mensagem enviada ", msg.TopicPartition)
    }

    producer.Flush(1000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "kafka:9092",
		"delivery.timeout.ms": "0",
		"acks": "all",
	}
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}
