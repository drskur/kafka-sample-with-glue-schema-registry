package org.example;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import example.Breed;
import example.Cat;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import kotlinx.serialization.SerializationException;

public class SampleProducer2 {
    public static void main(String[] args)
    {
        Properties props = getProperties();

        Cat cat = Cat.newBuilder()
                .setBreed(Breed.MAINE_COON)
                .setName("New Cat")
                .build();

        try (KafkaProducer<String, Cat> producer = new KafkaProducer<>(props))
        {
            final ProducerRecord<String, Cat> record = new ProducerRecord<String,Cat>("Cat2", cat);
            producer.send(record);
        }
        catch (final SerializationException e)
        {
            e.printStackTrace();
        }
    }

    private static Properties getProperties()
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");  
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");  
        return props;  
    }
}
