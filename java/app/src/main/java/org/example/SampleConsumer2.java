package org.example;

import example.Cat;  
import io.confluent.kafka.serializers.KafkaAvroDeserializer;  
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;  
import org.apache.kafka.clients.consumer.ConsumerConfig;  
import org.apache.kafka.clients.consumer.KafkaConsumer;  
import org.apache.kafka.common.serialization.StringDeserializer;  
import org.jetbrains.annotations.NotNull;  
  
import java.time.Duration;  
import java.util.Collections;  
import java.util.Properties;  
  
public class SampleConsumer2  
{  
    public static void main(String[] args) throws Exception  
    {  
        Properties properties = getProperties();  
  
        try (final KafkaConsumer<String, Cat> consumer = new KafkaConsumer<>(properties)) {  
            consumer.subscribe(Collections.singletonList("Cat2"));  
  
            while (true) {  
                consumer.poll(Duration.ofMillis(100)).forEach(record -> {  
                    final String key = record.key();  
                    final Cat cat = record.value();  
                    System.out.println("Received message: key = " + key + ", breed = " + cat.getBreed() + ", name = " + cat.getName());  
                });  
                consumer.commitAsync();  
            }  
        }  
  
    }  
  
    @NotNull  
    private static Properties getProperties() {  
        Properties props = new Properties();  
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");  
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());  
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());  
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "SampleTest2");  
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");  
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");  
  
        return props;  
    }  
}
