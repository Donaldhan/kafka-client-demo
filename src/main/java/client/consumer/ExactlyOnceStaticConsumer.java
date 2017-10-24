package client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import client.offset.OffsetManager;
import constant.BrokerConstant;
import util.PropertiesUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * To achieve exactly once scenario, offset should be manually managed (DO-NOT do auto offset management).
 * <p/>
 * The consumer can register with the Kafka broker with either (a) Auto load balance feature, this is
 * the scenario where if other consumers joins or fails. Kafka automatically re-balance the topic/partition
 * load with available consumers at that point. Or (b) Consumer register for a specific topic/partition, so no
 * automatic re-balance offered by Kafka at that point. Both these options (a & b) can be used in auto
 * or manual offset management.
 * <p/>
 * This example while demonstrates exactly once scenario, it also demonstrate above specific topic
 * partition registering option which is option (b) from the above. Please note that registering
 * for specific/static topic/partition is not mandatory to showcase exactly once scenario.
 * <p/>
 * Following are the steps for exactly once scenario by doing manual offset management and enabling
 * specific topic/partition registration.
 * <p/>
 * 1) enable.auto.commit = false
 * 2) Don't make call to consumer.commitSync(); after processing record.
 * 3) Register consumer to specific partition using 'assign' call.
 * 4) On start up of the consumer seek by consumer.seek(topicPartition,offset); to the offset where
 * you want to start reading from.
 * 5) Process records and get hold of the offset of each record, and store offset in an atomic way
 * along with the processed data using atomic-transaction to get the exactly-once semantic. (with data
 * in relational database this is easier) for non-relational  such as HDFS store the offset where the
 * data is since they don't support atomic transactions.
 * 6) Implement idempotent as a safety net.
 * <p/>
 *  Exactly once 每条消息肯定会被传输一次且仅传输一次，很多时候这是用户所想要的。
 *  Exactly once  消息传输保证模式消费者客户端;
 *  设置自动提交offset enable.auto.commit为false，将主题分区的offset信息保存在外部存储系统中，当前为
 *  文件系统，每次保存时，重写offset。在消息处理完毕之后，将主题分区offset信息保存保存到文件系统中，
 *  从而实现Exactly once 消息传输模式。
 *  每次读取消息时，手动使用consumer.seek(topicPartition,offset)方法定位主题分区的offset。
 */
public class ExactlyOnceStaticConsumer {
	private static final Logger log = LoggerFactory.getLogger(ExactlyOnceStaticConsumer.class);
	private static PropertiesUtil  propertiesUtil = PropertiesUtil.getInstance();
    private static OffsetManager offsetManager = new OffsetManager("offset-storage-static-consumer");
    public static void main(String[] str) throws InterruptedException, IOException {
    	log.info("Starting Manual Offset Guaranteed ExactlyOnce Reading From Specific Partition Consumer ...");
        readMessages();

    }
    /**
     * 每次读取消息时，手动使用consumer.seek(topicPartition,offset)方法定位主题分区的offset
     * @throws InterruptedException
     * @throws IOException
     */
    private static void readMessages() throws InterruptedException, IOException {
        KafkaConsumer<String, String> consumer = createConsumer();
        String topic = propertiesUtil.getProperty(BrokerConstant.TOPIC_NAME);
        log.info("ExactlyOnceStaticConsumer topic name:{}",topic);
        /*from the first partition consumer message,
         * if this partition no message can change another one,such as 1,2...
         */
        int partition = 0;
        TopicPartition topicPartition = registerConsumerToSpecificPartition(consumer, topic, partition);
        // Read the offset for the topic and partition from external storage.
        //从文件中，读取topic分区的offset
        long offset = offsetManager.readOffsetFromExternalStore(topic, partition);
        // Use seek and go to exact offset for that topic and partition.
        consumer.seek(topicPartition, offset);//定位主题分区的offset
        processRecords(consumer);

    }
    /**
     * 设置自动提交offset enable.auto.commit为false，将主题分区的offset信息保存在外部存储系统中，当前为
     * 文件系统，每次保存时，重写offset。在消息处理完毕之后，将主题分区offset信息保存保存到文件系统中，
     * 从而实现Exactly once 消息传输模式
     * @return
     */
    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        String bootstrapServers = propertiesUtil.getProperty(BrokerConstant.BOOTSTRAP_SERVERS);
        props.put("bootstrap.servers", bootstrapServers);
        String consumeGroup =  propertiesUtil.getProperty(BrokerConstant.AT_MOST_LEAST_ONCE_GROUP);
        props.put("group.id", consumeGroup);
        // Below is a key setting to turn off the auto commit.
        props.put("enable.auto.commit", "false");
        props.put("heartbeat.interval.ms", "2000");
        props.put("session.timeout.ms", "6001");
        // control maximum data on each poll, make sure this value is bigger than the maximum single record size
        props.put("max.partition.fetch.bytes", "140");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
    }
    /**
     * Manually listens for specific topic partition. But, if you are looking for example of how to dynamically listens
     * to partition and want to manually control offset then see ManualOffsetConsumerWithRebalanceExample.java
     */
    private static TopicPartition registerConsumerToSpecificPartition(KafkaConsumer<String, String> consumer, String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        List<TopicPartition> partitions = Arrays.asList(topicPartition);
        consumer.assign(partitions);
        return topicPartition;
    }
    /**
     * Process data and store offset in external store. Best practice is to do these operations atomically. Read class level comments.
     */
    private static void processRecords(KafkaConsumer<String, String> consumer) throws IOException {
    	log.info("start process records ....");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);//从主体poll 100个消息
            for (ConsumerRecord<String, String> record : records) {
                log.info("offset = {}, key = {}, value = {}\n", record.offset(), record.key(), record.value());
                //将分区的offset标志存放到消息所在的topic 分区对应的文件中
                offsetManager.saveOffsetInExternalStore(record.topic(), record.partition(), record.offset());

            }
        }
    }
}
