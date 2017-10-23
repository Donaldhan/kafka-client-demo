package client.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import client.offset.OffsetManager;

import java.util.Collection;

/**
 * Re-balancer for any subscription changes.
 */
public class MyConsumerRebalancerListener implements org.apache.kafka.clients.consumer.ConsumerRebalanceListener {
	private OffsetManager offsetManager = new OffsetManager("storage2");
	private Consumer<String, String> consumer;
	public MyConsumerRebalancerListener(Consumer<String, String> consumer) {
		this.consumer = consumer;
	}
	/* (non-Javadoc)
	 * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(java.util.Collection)
	 */
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

		for (TopicPartition partition : partitions) {
			offsetManager.saveOffsetInExternalStore(partition.topic(), partition.partition(),
					consumer.position(partition));
		}
	}
	/* (non-Javadoc)
	 * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(java.util.Collection)
	 */
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		for (TopicPartition partition : partitions) {
			consumer.seek(partition,
					offsetManager.readOffsetFromExternalStore(partition.topic(), partition.partition()));
		}
	}
}
