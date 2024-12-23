package org.java.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * The KafkaProducerApp class is responsible for producing messages to a Kafka topic.
 * It demonstrates the creation of a Kafka producer, sending messages to a specified topic,
 * and managing resources.
 */
public class KafkaProducerApp {

    private static final String TOPIC = "events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";

    /**
     * Produces a specified number of messages to a Kafka topic using the provided Kafka producer.
     *
     * @param numberOfMessages the number of messages to be produced and sent to the topic
     * @param kafkaProducer the Kafka producer used to send messages to the topic
     */
    public static void produceMessages(int numberOfMessages, Producer<Long, String> kafkaProducer) {
        int partition = 0;
        for (int i = 0; i < numberOfMessages; i++) {
            long key = i;
            String value = String.format("event %d", key);
            long timestamp = System.currentTimeMillis();
            // if we don't specify the partition, kafka will choose for every message a partition based on key has algorithm
            // if we don't specify the partition and keys, kafka will use round robbin to balance messages to the available partitions
            ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, partition, timestamp, key, value);
            try {
                RecordMetadata recordMetadata = kafkaProducer.send(record).get();
                System.out.println(String.format("Record with (key: %s, value: %s), was sent to (partition: %d, timestamp: %d, offset: %d)",
                        record.key(), record.value(), partition, timestamp, recordMetadata.offset()));
            }
            catch (InterruptedException e) {
            }
            catch (ExecutionException e) {
            }
        }
    }

    /**
     * Creates and configures a Kafka producer instance for sending messages to a Kafka cluster.
     *
     * @param bootstrapServers a comma-separated list of host:port pairs of the Kafka brokers
     *                         that the producer will connect to
     * @return a configured Kafka producer instance capable of sending messages with a key of type Long
     *         and a value of type String
     */
    public static Producer<Long, String> createKafkaProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "events-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /**
     * The main method serves as the entry point for the KafkaProducerApp.
     * It creates a Kafka producer, produces a predefined number of messages to a Kafka topic,
     * and ensures proper resource management by flushing and closing the producer.
     *
     * @param args command-line arguments passed to the program
     */
    public static void main(String[] args) {
        Producer<Long, String> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);
        produceMessages(10, kafkaProducer);
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
