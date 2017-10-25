package client.producer.arvo;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import client.avro.AvroSupport;
import constant.BrokerConstant;
import util.PropertiesUtil;

import java.io.IOException;
import java.util.Properties;

/**
 * Create and publish an avro type message.
 */
public class AvroProducerExample {
	private static final Logger log = LoggerFactory.getLogger(AvroProducerExample.class);
	private static PropertiesUtil  propertiesUtil = PropertiesUtil.getInstance();
    public static void main(String[] str) throws InterruptedException, IOException {
    	log.info("Starting ProducerAvroExample ...");
        sendMessages();
    }

    /**
     * @throws InterruptedException
     * @throws IOException
     */
    private static void sendMessages() throws InterruptedException, IOException {
        Producer<String, byte[]> producer = createProducer();
        sendRecords(producer);
    }

    /**
     * @return
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
	private static Producer<String, byte[]> createProducer() {
        Properties props = new Properties();
        String bootstrapServers = propertiesUtil.getProperty(BrokerConstant.BOOTSTRAP_SERVERS);
        props.put("bootstrap.servers", bootstrapServers);//localhost:9092
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer(props);
    }
    /**
     * @param producer
     * @throws IOException
     * @throws InterruptedException
     */
    private static void sendRecords(Producer<String, byte[]> producer) throws IOException, InterruptedException {
    	String topic = propertiesUtil.getProperty(BrokerConstant.AVRO_TOPIC);
    	log.info("AvroProducerExample topic name:{}",topic);
        int partition = 0;
        while (true) {
        	log.info("send 100 message start ...");
            for (int i = 1; i <= 100; i++)
            {
            	 ProducerRecord<String, byte[]> message = new ProducerRecord<String, byte[]>(topic, partition, Integer.toString(i), record(i + ""));
            	 producer.send(message);
            }
            log.info("send 100 message end ...");
            Thread.sleep(500);
        }
    }


    /**
     * 使用arvo，将name String转换成字节数组
     * @param name
     * @return
     * @throws IOException
     */
    private static byte[] record(String name) throws IOException {
        GenericRecord record = new GenericData.Record(AvroSupport.getSchema());
        record.put("firstName", name);
        return AvroSupport.dataToByteArray(AvroSupport.getSchema(), record);

    }


}
