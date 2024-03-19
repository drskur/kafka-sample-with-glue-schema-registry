package org.example;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;  
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;  
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;  
import example.Cat;  
import org.apache.kafka.clients.consumer.ConsumerConfig;  
import org.apache.kafka.clients.consumer.KafkaConsumer;  
import org.apache.kafka.common.serialization.StringDeserializer;  
import org.jetbrains.annotations.NotNull;  
import software.amazon.awssdk.services.glue.model.DataFormat;  
  
import java.time.Duration;  
import java.util.Collections;  
import java.util.Properties;  
  
public class SampleConsumer  
{  
    public static void main(String[] args) throws Exception  
    {  
        Properties properties = getProperties();  
  
        try (final KafkaConsumer<String, Cat> consumer = new KafkaConsumer<>(properties)) {  
            consumer.subscribe(Collections.singletonList("Cat"));  
  
            while (true) {  
                consumer.poll(Duration.ofMillis(100)).forEach(record -> {  
                    final String key = record.key();  
                    final Cat cat = record.value();  
                    System.out.println("Received message: key = " + key + ", breed = " + cat.getBreed());  
                });  
                consumer.commitAsync();;  
            }  
        }  
  
    }  
  
    @NotNull  
    private static Properties getProperties() {  
        Properties props = new Properties();  
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");  
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());  
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class.getName());  
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "SampleTest2");  
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "ap-northeast-2");  
        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());  
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");  
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "msk-test");  
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());  
  
        return props;  
    }  
}
