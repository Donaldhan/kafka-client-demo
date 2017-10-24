package client.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import client.listener.MyConsumerRebalancerListener;
import client.offset.OffsetManager;
import constant.BrokerConstant;
import util.PropertiesUtil;

import java.util.Arrays;
import java.util.Properties;

/**
 * To achieve exactly once scenario, offset should be manually managed (DO-NOT do auto offset management).
 * <p/>
 * The consumer can register with the Kafka broker with either (a) Auto load balance feature, this is
 * the scenario where if other consumers joins or fails. Kafka automatically re-balance the topic/partition
 * load with available consumers at that point. Or (b) Consumer register for a specific topic/partition,
 * so no automatic re-balance offered by Kafka at that point. Both these options (a & b) can be used
 * in auto or manual offset management.
 * <p/>
 * This example while demonstrates exactly once scenario, it also demonstrate above auto re-balance
 * option which is option (a) feature from the above. Please note that registering for auto re-balance
 * to topic/partition is not mandatory to showcase exactly once scenario.
 * <p/>
 * Following are the steps for exactly once scenario by doing manual offset management and enabling
 * automatic re-balance.
 * <p/>
 * 1) enable.auto.commit = false
 * 2) Don't make call to consumer.commitSync(); after processing record.
 * 3) Register consumer to topics to get dynamically assigned partitions using 'subscribe' call.
 * 4) Implement a ConsumerRebalanceListener and seek by calling consumer.seek(topicPartition,offset);
 * to start reading from for a specific partition.
 * 5) Process records and get hold of the offset of each record, and store offset in an atomic way along
 * with the processed data  using atomic-transaction to get the exactly-once semantic. (with data in
 * relational database this is easier) for non-relational  such as HDFS store the offset where the data
 * is since they don't support atomic transactions.
 * 6) Implement idempotent as a safety net.
 * <p/>
 *  Exactly once 每条消息肯定会被传输一次且仅传输一次，很多时候这是用户所想要的。
 *  Exactly once  消息传输保证模式消费者客户端;
 *  设置自动提交offset enable.auto.commit为false，将主题分区的offset信息保存在外部存储系统中，当前为
 *  文件系统，每次保存时，重写offset。在消息处理完毕之后，将主题分区offset信息保存保存到文件系统中，
 *  从而实现Exactly once 消息传输模式。
 * 手动控制offset，在每次注册消费者到主题时，动态指定分区后，消费者负载监听器ConsumerRebalancerListener，
 * 是用consumer.seek(topicPartition,offset)方法控制offset。
 */
public class ExactlyOnceDynamicConsumer {
	private static final Logger log = LoggerFactory.getLogger(ExactlyOnceDynamicConsumer.class);
	private static PropertiesUtil  propertiesUtil = PropertiesUtil.getInstance();
    private static OffsetManager offsetManager = new OffsetManager("offset-storage-dynamic-consumer");
    public static void main(String[] str) throws InterruptedException {
    	log.info("Starting Manual Offset Guaranteed Exactly Once Reading Dynamically Balanced Partition Consumer ...");
        readMessages();
    }
    /**
     * 手动控制offset，在每次注册消费者到主题时，动态指定分区后，消费者负载监听器ConsumerRebalancerListener，
     * 是用consumer.seek(topicPartition,offset)方法控制offset。
     * @throws InterruptedException
     */
    private static void readMessages() throws InterruptedException {
        KafkaConsumer<String, String> consumer = createConsumer();
        String topic = propertiesUtil.getProperty(BrokerConstant.TOPIC_NAME);
        // Manually controlling offset but register consumer to topics to get dynamically assigned partitions.
        // Inside MyConsumerRebalancerListener use consumer.seek(topicPartition,offset) to control offset
        consumer.subscribe(Arrays.asList(topic), new MyConsumerRebalancerListener(consumer,offsetManager));
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
        String consumeGroup = "cg3";
        props.put("group.id", consumeGroup);
        // Below is a key setting to turn off the auto commit.
        props.put("enable.auto.commit", "false");
        props.put("heartbeat.interval.ms", "2000");
        props.put("session.timeout.ms", "6001");
        // Control maximum data on each poll, make sure this value is bigger than the maximum single record size
        props.put("max.partition.fetch.bytes", "140");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
    }

    /**
     * @param consumer
     */
    private static void processRecords(KafkaConsumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                log.info("offset = {}, key = {}, value = {}\n", record.offset(), record.key(), record.value());
                //将分区的offset标志存放到消息所在的topic 分区对应的文件中
                offsetManager.saveOffsetInExternalStore(record.topic(), record.partition(), record.offset());
            }
        }
    }
}
