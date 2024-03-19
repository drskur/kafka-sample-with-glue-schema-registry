package org.example;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;  
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;  
import example.Breed;  
import example.Cat;  
import kotlinx.serialization.SerializationException;  
import org.apache.kafka.clients.producer.KafkaProducer;  
import org.apache.kafka.clients.producer.ProducerConfig;  
import org.apache.kafka.clients.producer.ProducerRecord;  
import org.apache.kafka.common.serialization.StringSerializer;  
import org.jetbrains.annotations.NotNull;  
import software.amazon.awssdk.services.glue.model.DataFormat;  
import java.util.Properties;  
  
public class SampleProducer {  
  
    public static void main(String[] args) throws Exception  
    {  
        Properties props = getProperties();  
  
        Cat cat = Cat.newBuilder()  
                .setBreed(Breed.MAINE_COON)  
                .setName("Hello World")  
                .build();  
  
        try (KafkaProducer<String, Cat> producer = new KafkaProducer<>(props)) {  
  
            final ProducerRecord<String, Cat> record = new ProducerRecord<>("Cat", cat);  
            producer.send(record);  
  
        } catch (final SerializationException e) {  
            e.printStackTrace();  
        }  
  
    }  
  
    @NotNull  
    private static Properties getProperties() {  
        Properties props = new Properties();  
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");  
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());  
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());  
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "ap-northeast-2");  
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());  
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");  
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "msk-test");  
        return props;  
    }  
  
}