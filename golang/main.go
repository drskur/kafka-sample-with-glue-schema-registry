package main

import (  
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"  
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"  
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"  
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"  
    avro2 "kafka-sample/avro"  
    "log"  
)  

func main() {  
  
    topic := "Cat2"  
  
    p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:29092"})  
    if err != nil {  
       log.Fatal(err)  
    }  
  
    client, err := schemaregistry.NewClient(schemaregistry.NewConfig("http://localhost:8081"))  
    if err != nil {  
       log.Fatal(err)  
    }  
  
    ser, err := avro.NewSpecificSerializer(client, serde.ValueSerde, avro.NewSerializerConfig())  
    if err != nil {  
       log.Fatal(err)  
    }  
  
    cat := avro2.Cat{  
       Breed: avro2.BreedRAGDOLL,  
       Name:  "Yahong YYU",  
    }  
  
    payload, err := ser.Serialize(topic, &cat)  
    if err != nil {  
       log.Fatal(err)  
    }  
  
    ch := make(chan kafka.Event)  
    err = p.Produce(&kafka.Message{  
       TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},  
       Value:          payload,  
    }, ch)  
  
    if err != nil {  
       log.Fatal(err)  
    }  
  
    e := <-ch  
    m := e.(*kafka.Message)  
  
    if m.TopicPartition.Error != nil {  
       log.Fatalf("Delivery failed: %v\n", m.TopicPartition.Error)  
    } else {  
       log.Printf("Delivered message to topic %s [%d] at offset %v\n",  
          *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)  
    }  
  
    close(ch)  
}