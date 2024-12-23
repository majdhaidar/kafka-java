package org.java.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;

public class KafkaConsumerApp {


    private static final String TOPIC = "events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";
    private static final String CONSUMER_GROUP_ID = "events-consumer";

    /**
     * Consumes messages from the specified Kafka topic using the provided Kafka consumer.
     * This method subscribes to the given topic, continuously polls for new messages,
     * processes them, and commits offsets asynchronously.
     *
     * @param topic the name of the Kafka topic from which messages will be consumed
     * @param kafkaConsumer the Kafka consumer instance used to consume messages
     */
    public static void consumeMessages(String topic, Consumer<Long, String> kafkaConsumer) {
        //subscribe to topic
        kafkaConsumer.subscribe(java.util.Collections.singletonList(topic));

        while(true){
            ConsumerRecords<Long, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            if(records.isEmpty()){
                // do something else
            }

            for(ConsumerRecord<Long, String> record : records){
                System.out.println(String.format("Received record with (key: %s, value: %s, partition: %d, offset: %d)", record.key(), record.value(), record.partition(), record.offset()));
            }

            kafkaConsumer.commitAsync();
        }
    }

    /**
     * Creates and configures a Kafka consumer instance for consuming messages from a Kafka cluster.
     *
     * @param bootstrapServers a comma-separated list of host:port pairs of the Kafka brokers
     *                         that the consumer will connect to
     * @param consumerGroupId the unique identifier of the consumer group to which this consumer belongs
     * @return a configured Kafka consumer instance capable of consuming messages with a key of type Long
     *         and a value of type String
     */
    public static Consumer<Long, String> createKafkaConsumer(String bootstrapServers, String consumerGroupId){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<Long, String>(properties);
    }

    public static void main(String[] args) {
        Consumer<Long, String> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS, CONSUMER_GROUP_ID);
        consumeMessages(TOPIC, kafkaConsumer);
    }
}
