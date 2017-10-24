package client.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import constant.BrokerConstant;
import util.PropertiesUtil;

import java.io.IOException;
import java.util.Properties;
/**
 * A sample client to produce a bunch of messages.
 * 消息生产者
 */
public class ProducerExample {
	private static final Logger log = LoggerFactory.getLogger(ProducerExample.class);
	private static PropertiesUtil  propertiesUtil = PropertiesUtil.getInstance();
    public static void main(String[] str) throws InterruptedException, IOException {
    	log.info("Starting ProducerExample ...");
        sendMessages();
    }
    /**
     * @throws InterruptedException
     * @throws IOException
     */
    private static void sendMessages() throws InterruptedException, IOException {
        Producer<String, String> producer = createProducer();
        sendMessages(producer);
        // Allow the producer to complete the sending of the records before existing the program.
        Thread.sleep(1000);

    }

    /**
     * @return
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
	private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        String bootstrapServers = propertiesUtil.getProperty(BrokerConstant.BOOTSTRAP_SERVERS);
        props.put("bootstrap.servers", bootstrapServers);//localhost:9092
        props.put("acks", "all");
        props.put("retries", 0);
        // This property controls how much bytes the sender would wait to batch up the content before publishing to Kafka.
        props.put("batch.size", 10);
        props.put("linger.ms", 1);
        String keySerializer = propertiesUtil.getProperty(BrokerConstant.KEY_SERIALIZER);
        props.put("key.serializer", keySerializer);
        String valueSerializer = propertiesUtil.getProperty(BrokerConstant.VALUE_SERIALIZER);
        props.put("value.serializer", valueSerializer);
        return new KafkaProducer(props);
    }

    /**
     * 
     * @param producer
     */
    private static void sendMessages(Producer<String, String> producer) {
        String topic = propertiesUtil.getProperty(BrokerConstant.TOPIC_NAME);
        int partition = 0;
        long record = 1;
        for (int i = 1; i <= 10; i++) {
        	//ProducerRecord的构造参数，一次为主题名，分区，消息key，消息value
        	ProducerRecord<String, String> message = new ProducerRecord<String, String>(topic, partition, Long.toString(record), Long.toString(record++));
            producer.send(message);
        }
        log.info("send message ended...");
    }


}
