package client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import constant.BrokerConstant;
import util.PropertiesUtil;

import java.util.Arrays;
import java.util.Properties;
/**
 * To set this up scenario set ‘enable.auto.commit’ to true and set ‘auto.commit.interval.ms’
 * to a higher number and committing offsets by a call to consumer.commitSync(); after consumer
 * process the message
 * <p/>
 * Read section 'Manual Offset Control'.
 * http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
 * <p/>
 * When consumer is of this type, try to implement 'idempotent' to avoid processing the same record again
 * using some record key or hash during the scenario when message are processed twice.
 * <p/>
 * <p/>
 * At least one 消息绝不会丢，但可能会重复传输;
 * At-least-once 消息传输保证模式消费者客户端;
 * KafkaConsumer消费者客户端主要通过设置enable.auto.commit为true，同时设置
 * auto.commit.interval.ms时间为尽可能的大，在每次处理消息完，通过consumer.commitSync()方法，提交offset，
 * 从而实现at-least-once 消息传输保证模式
 */
public class AtLeastOnceConsumer {
	private static final Logger log = LoggerFactory.getLogger(AtLeastOnceConsumer.class);
	private static PropertiesUtil  propertiesUtil = PropertiesUtil.getInstance();
    public static void main(String[] str) throws InterruptedException {
    	log.info("Starting AutoOff set Guranteed AtLeastOnce Consumer ...");
        execute();
    }
    /**
     * @throws InterruptedException
     */
    private static void execute() throws InterruptedException {
        KafkaConsumer<String, String> consumer = createConsumer();
        String topic = propertiesUtil.getProperty(BrokerConstant.TOPIC_NAME);
        log.info("AtLeastOnceConsumer topic name:{}",topic);
        // Subscribe to all partition in that topic. 'assign' could be used here
        // instead of 'subscribe' to subscribe to specific partition.
        consumer.subscribe(Arrays.asList(topic));
        processRecords(consumer);

    }
    /**
     * KafkaConsumer消费者客户端主要通过设置enable.auto.commit为true，同时设置
     * auto.commit.interval.ms时间为尽可能的大，在每次处理消息完，通过consumer.commitSync()方法，提交offset，
     * 从而实现at-least-once 消息传输保证模式
     * @return
     */
    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        String bootstrapServers = propertiesUtil.getProperty(BrokerConstant.BOOTSTRAP_SERVERS);
        props.put("bootstrap.servers", bootstrapServers);
        String consumeGroup = propertiesUtil.getProperty(BrokerConstant.AT_MOST_LEAST_ONCE_GROUP);
        props.put("group.id", consumeGroup);
        // Set this property, if auto commit should happen.
        props.put("enable.auto.commit", "true");
        // Make Auto commit interval to a big number so that auto commit does not happen,
        // we are going to control the offset commit via consumer.commitSync(); after processing record.
        props.put("auto.commit.interval.ms", "999999999");
        // This is how to control number of records being read in each poll
        props.put("max.partition.fetch.bytes", "135");
        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "6001");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
    }
    /**
     * @param consumer
     * @throws InterruptedException
     */
    private static void processRecords(KafkaConsumer<String, String> consumer) throws InterruptedException {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            long lastOffset = 0;
            for (ConsumerRecord<String, String> record : records) {
            	log.info("\n\roffset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                lastOffset = record.offset();
            }
            log.info("lastOffset read:{} " ,lastOffset);
            process();
            // Below call is important to control the offset commit. Do this call after you
            // finish processing the business process to get the at least once guarantee.
            consumer.commitSync();
        }
    }
    /**
     * @throws InterruptedException
     */
    private static void process() throws InterruptedException {
        // create some delay to simulate processing of the record.
        Thread.sleep(500);
    }
}
