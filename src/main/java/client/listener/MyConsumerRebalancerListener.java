package client.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import client.offset.OffsetManager;

import java.util.Collection;

/**
 * ConsumerRebalanceListener see#
 * http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
 * ConsumerRebalanceListener , A callback interface that the user can implement to trigger custom actions 
 * when the set of partitions assigned to the consumer changes.
 * <p/>
 * This is applicable when the consumer is having Kafka auto-manage group membership. 
 * If the consumer's directly assign partitions, those partitions will never be reassigned and this callback is not applicable.
 * <p/>
 * When Kafka is managing the group membership, a partition re-assignment will be triggered 
 * any time the members of the group changes or the subscription of the members changes. 
 * This can occur when processes die, new process instances are added or old instances come 
 * back to life after failure. Rebalances can also be triggered by changes affecting the subscribed 
 * topics (e.g. when then number of partitions is administratively adjusted).
 * <p/>
 * There are many uses for this functionality. One common use is saving offsets in a custom store. 
 * By saving offsets in the onPartitionsRevoked(Collection), call we can ensure that any time partition 
 * assignment changes the offset gets saved.
 * <p/>
 * Another use is flushing out any kind of cache of intermediate results the consumer may be keeping. 
 * For example, consider a case where the consumer is subscribed to a topic containing user page views, 
 * and the goal is to count the number of page views per users for each five minute window. Let's say 
 * the topic is partitioned by the user id so that all events for a particular user will go to a single 
 * consumer instance. The consumer can keep in memory a running tally of actions per user and only flush 
 * these out to a remote data store when its cache gets to big. However if a partition is reassigned it 
 * may want to automatically trigger a flush of this cache, before the new owner takes over consumption.
 * <p/>
 * This callback will execute in the user thread as part of the poll(long) call whenever partition assignment changes.
 * <p/>
 * It is guaranteed that all consumer processes will invoke onPartitionsRevoked prior to any process
 * invoking onPartitionsAssigned. So if offsets or other state is saved in the onPartitionsRevoked call 
 * it is guaranteed to be saved by the time the process taking over that partition has their onPartitionsAssigned 
 * callback called to load the state. 
 * <p/>
 * Re-balancer for any subscription changes.
 * 消费者负载均衡监听器，主要是动态保存主题分区的offset信息和定位主题各分区的offset位置
 */
public class MyConsumerRebalancerListener implements org.apache.kafka.clients.consumer.ConsumerRebalanceListener {
	private OffsetManager offsetManager;
	private Consumer<String, String> consumer;
	public MyConsumerRebalancerListener(Consumer<String, String> consumer,OffsetManager offsetManager) {
		this.consumer = consumer;
		this.offsetManager = offsetManager;
	}
    /**
     * A callback method the user can implement to provide handling of offset commits to a customized store
     * on the start of a rebalance operation. This method will be called before a rebalance operation starts
     * and after the consumer stops fetching data. It is recommended that offsets should be committed in this
     * callback to either Kafka or a custom offset store to prevent duplicate data. 
     * 保存主题分区的offset信息
     */
	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		for (TopicPartition partition : partitions) {
			offsetManager.saveOffsetInExternalStore(partition.topic(), partition.partition(),
					consumer.position(partition));
		}
	}
	/**
	 * A callback method the user can implement to provide handling of customized offsets on completion 
	 * of a successful partition re-assignment. 
	 * This method will be called after an offset re-assignment completes and before the consumer starts fetching data.
	 * It is guaranteed that all the processes in a consumer group will execute their onPartitionsRevoked(Collection) 
	 * callback before any instance executes its onPartitionsAssigned(Collection) callback.
	 * 定位主题各分区的offset位置
	 */
	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		for (TopicPartition partition : partitions) {
			consumer.seek(partition,
					offsetManager.readOffsetFromExternalStore(partition.topic(), partition.partition()));
		}
	}
}
