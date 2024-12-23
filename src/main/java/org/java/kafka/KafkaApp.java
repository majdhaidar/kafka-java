package org.java.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaApp {

    private static final String TOPIC = "events";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";

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

    public static Producer<Long, String> createKafkaProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "events-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void main(String[] args) {
        Producer<Long, String> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);
        produceMessages(10, kafkaProducer);
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
