package client.consumer.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import client.avro.AvroSupport;
import client.producer.arvo.AvroProducerExample;
import constant.BrokerConstant;
import util.PropertiesUtil;

import java.util.Arrays;
import java.util.Properties;

/**
 * Reads an avro message.
 */
public class AvroConsumerExample {
	private static final Logger log = LoggerFactory.getLogger(AvroProducerExample.class);
	private static PropertiesUtil  propertiesUtil = PropertiesUtil.getInstance();
    public static void main(String[] str) throws InterruptedException {
    	log.info("Starting Auto Offset AvroConsumerExample ...");
        readMessages();
    }

    /**
     * @throws InterruptedException
     */
    private static void readMessages() throws InterruptedException {
        KafkaConsumer<String, byte[]> consumer = createConsumer();
        String topic = propertiesUtil.getProperty(BrokerConstant.AVRO_TOPIC);
        log.info("AvroConsumerExample topic name:{}",topic);
        // Assign to specific topic and partition, subscribe could be used here to subscribe to all topic.
        consumer.assign(Arrays.asList(new TopicPartition(topic, 0)));
        processRecords(consumer);
    }

    /**
     * @param consumer
     * @throws InterruptedException
     */
    private static void processRecords(KafkaConsumer<String, byte[]> consumer) throws InterruptedException {
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            long lastOffset = 0;
            for (ConsumerRecord<String, byte[]> record : records) {
                GenericRecord genericRecord = AvroSupport.byteArrayToData(AvroSupport.getSchema(), record.value());
                String firstName = AvroSupport.getValue(genericRecord, "firstName", String.class);
                log.info("\n\roffset = {}, key = {}, value = {}", record.offset(), record.key(), firstName);
                lastOffset = record.offset();
            }
            log.info("lastOffset read: " + lastOffset);
            consumer.commitSync();
            Thread.sleep(500);

        }
    }
    /**
     * @return
     */
    private static KafkaConsumer<String, byte[]> createConsumer() {
        Properties props = new Properties();
        String bootstrapServers = propertiesUtil.getProperty(BrokerConstant.BOOTSTRAP_SERVERS);
        props.put("bootstrap.servers", bootstrapServers);//localhost:9092
        String consumeGroup = propertiesUtil.getProperty(BrokerConstant.AT_MOST_LEAST_ONCE_GROUP);
        props.put("group.id", consumeGroup);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "100");
        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return new KafkaConsumer<String, byte[]>(props);
    }
}
