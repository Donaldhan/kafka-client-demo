package constant;

/**
 * broker configuration constant
 * @author donald
 * 2017年10月24日
 * 上午8:31:51
 */
public class BrokerConstant {
	/**
	 * broker 地址
	 */
	public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
	/**
	 * key序列化类
	 */
	public static final String KEY_SERIALIZER = "key.serializer";
	/**
	 * value序列化类
	 */
	public static final String VALUE_SERIALIZER = "value.serializer";
	/**
	 * 主题名
	 */
	public static final String TOPIC_NAME = "topic";
	/**
	 * key反序列化类
	 */
	public static final String KEY_DESERIALIZER = "key.deserializer";
	/**
	 * value反序列化类
	 */
	public static final String VALUE_DESERIALIZER = "key.deserializer";
	/**
	 * At Most Once and At Least Once kafka  delivery guarantee mode group
	 */
	public static final String AT_MOST_LEAST_ONCE_GROUP = "atMostLeastOnce.group";
	/**
	 * Exactly Once kafka  delivery guarantee mode group
	 */
	public static final String EXACTLYONCE_GROUP = "exactlyOnce.group";
	/**
	 * avro topic name
	 */
	public static final String AVRO_TOPIC = "avroTopic";
	
}
