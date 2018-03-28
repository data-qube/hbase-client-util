package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class MyKafkaConsumer extends Thread {
	// 消费者连接
	private final ConsumerConnector consumer;
	// 要消费的话题
	private final String topic;

	private final String groupid;

	public MyKafkaConsumer(String topic, String groupid) {
		this.topic = topic;
		this.groupid = groupid;
		this.consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(groupid));
	}

	// config info
	private static ConsumerConfig createConsumerConfig(String groupid) {
		Properties props = new Properties();
		props.put("zookeeper.connect", "192.168.130.141:2181");
		props.put("group.id", groupid);
		props.put("zookeeper.session.timeout.ms", "10000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	public void run() {

		Map<String, Integer> topickMap = new HashMap<String, Integer>();
		topickMap.put(topic, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer.createMessageStreams(topickMap);

		KafkaStream<byte[], byte[]> stream = streamMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		System.out.println("*********" + groupid + "********");
		long startTime = System.currentTimeMillis();
		int count = 0;
		while (true) {
			if (it.hasNext()) {
				// print out the received message
//				MessageAndMetadata value = it.next();
//
//				System.err.println(Thread.currentThread() + " get offset:" + value.offset());
//				// System.err.println(Thread.currentThread() + " get key:" + new
//				// String(it.next().key()));
//
//				System.err.println(Thread.currentThread() + " get data:" + new String((byte[]) value.message()));
				it.next().message();
				count++;
				if(count == 100000)
				{
					System.out.println(count);
					long endTime = System.currentTimeMillis();
					System.out.println(endTime - startTime);
					break;
				}
			}
		}
	}

	public static void main(String[] args) {
		MyKafkaConsumer consumerThread = new MyKafkaConsumer("flume_hbase_3", "0");
		consumerThread.start();
	}
}
