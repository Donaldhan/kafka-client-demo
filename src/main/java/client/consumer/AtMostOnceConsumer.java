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
 * At-most-once is the default behavior of KAFKA, Depending on how consumer is configured for auto
 * offset management, in some cases this message pattern would turn into at-least-once rather than at-most-once.
 * <p/>
 * Since at-most-once is the lower messaging guarantee we would declare this consumer as at-most-once.
 * <p/>
 * To get to this behavior ‘enable.auto.commit’ to true and set ‘auto.commit.interval.ms’ to a lower time-frame.
 * And do not make call to consumer.commitSync(); from the consumer. Now Kafka would auto commit offset at
 * the specified interval.
 * <p/>
 * <p/>
 * Below you would find explanation of when consumer behaves as at-most-once and when it behaves as
 * at-least-once.
 * <p/>
 * To get to this behavior set 'auto.commit.interval.ms' to a lower time-frame. Do not make call to
 * consumer.commitSync(); from the consumer. Now Kafka would auto commit offset at the specified interval.
 * <p/>
 * 'At-most-once' consumer behaviour happens as explained below.
 * <p/>
 * <ol>
 * <li> The commit interval passes and Kafka commits the offset, but client did not complete the
 * processing of the message and client crashes. Now when client restarts it looses the committed message.
 * </li>
 * <p/>
 * 'At-least-once' scenario is below.
 * <p/>
 * <li>Client processed a message and committed to its persistent store. But the Kafka commit interval is NOT
 * passed and kafka could not commit the offset. At this point clients dies, now when client restarts it
 * re-process the same message again.</li>
 * </ol>
 * <p/>
 * </ol>
 * At most once 消息可能会丢，但绝不会重复传输;
 * At-most-once 消息传输保证模式消费者客户端;
 * KafkaConsumer消费者客户端主要通过设置enable.auto.commit为true，同时在
 * auto.commit.interval.ms时间间隔后提交offset，从而实现At-most-once 消息传输保证模式。
 */
public class AtMostOnceConsumer {
	private static final Logger log = LoggerFactory.getLogger(AtMostOnceConsumer.class);
	private static PropertiesUtil  propertiesUtil = PropertiesUtil.getInstance();
	
    public static void main(String[] str) throws InterruptedException {
    	log.info("Starting Auto Offset Mostly AtleastOnce But Sometime AtMostOnce Consumer ...");
        execute();

    }

    /**
     * @throws InterruptedException
     */
    private static void execute() throws InterruptedException {
        KafkaConsumer<String, String> consumer = createConsumer();
        String topic = propertiesUtil.getProperty(BrokerConstant.TOPIC_NAME);
        log.info("AtMostOnceConsumer topic name:{}",topic);
        // Subscribe to all partition in that topic. 'assign' could be used here
        // instead of 'subscribe' to subscribe to specific partition.
        consumer.subscribe(Arrays.asList(topic));
        processRecords(consumer);
    }

    /**
     * KafkaConsumer消费者客户端主要通过设置enable.auto.commit为true，同时在
     * auto.commit.interval.ms时间间隔后提交offset，从而实现At-most-once 消息传输保证模式
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
        // Auto commit interval is an important property, kafka would commit offset at this interval.
        props.put("auto.commit.interval.ms", "101");
        // This is how to control number of records being read in each poll
        props.put("max.partition.fetch.bytes", "135");
        // Set this if you want to always read from beginning.
        //        props.put("auto.offset.reset", "earliest");
        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "6001");
        String keyDeserializer = propertiesUtil.getProperty(BrokerConstant.KEY_DESERIALIZER);
        props.put("key.deserializer", keyDeserializer);
        String valueDeserializer = propertiesUtil.getProperty(BrokerConstant.VALUE_DESERIALIZER);
        props.put("value.deserializer", valueDeserializer);
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
            log.info("lastOffset read: {}",lastOffset);
            process();
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
