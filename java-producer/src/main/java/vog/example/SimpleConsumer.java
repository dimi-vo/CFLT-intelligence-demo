package vog.example;

import datagen.example.purchase;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class SimpleConsumer {

    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.load();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, dotenv.get("BOOTSTRAP_SERVERS"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, dotenv.get("GROUP_ID"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"" + dotenv.get("KAFKA_API_KEY") + "\" " +
                        "password=\"" + dotenv.get("KAFKA_API_SECRET") + "\";");

        props.put("schema.registry.url", dotenv.get("SCHEMA_REGISTRY_URL"));
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info",
                dotenv.get("SCHEMA_REGISTRY_API_KEY") + ":" + dotenv.get("SCHEMA_REGISTRY_API_SECRET"));
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        String topic = dotenv.get("TOPIC");

        try (KafkaConsumer<String, purchase> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            log.info("Subscribed to topic: {}", topic);

            while (true) {
                ConsumerRecords<String, purchase> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record ->
                        log.info("key={}, value={}, partition={}, offset={}",
                                record.key(), record.value(), record.partition(), record.offset())
                );
            }
        }
    }
}
