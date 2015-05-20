package com.autohome.sync.syncCluster.consumers;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import backtype.storm.utils.Utils;

import com.autohome.sync.syncCluster.producers.ProducerConf;
import com.autohome.sync.syncCluster.producers.SyncProducer;
import com.autohome.sync.syncCluster.tools.Configuration;
import com.autohome.sync.syncCluster.tools.DynamicPartitionConnections;
import com.autohome.sync.syncCluster.tools.UpdateOffsetException;
import com.autohome.sync.syncCluster.tools.ZKState;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;

public class ConsumerReplica implements Runnable {

	private Long _emittedToOffset; // 从kafka读到的offset,
	// 从kafka读到的messages会放入_waitingToEmit,放入这个list,
	// 我们就认为一定会被emit,
	// 所以emittedToOffset可以认为是从kafka读到的offset.
	private Long _committedTo; // 已经写入zk的offset
	private LinkedList<MessageAndOffset> _waitingToEmit = new LinkedList<MessageAndOffset>();
	private int _partition;
	private String _curoffset;
	private Long currentOffset;
	private SimpleConsumer _consumer;
	private List<kafka.producer.KeyedMessage<String, Message>> list = null;
	private SyncProducer producer;
	private long lastRefreshTimeMs;
	private long refreshMillis;
	private DynamicBrokersReader _reader;
	private String path;
	private Configuration _conf;

	/**
	 * 1. zookeeper上面存储offset的路径为: /sync/consumers/topics/[topic]/offset/0...n
	 * 
	 * @param connections
	 * @param topologyInstanceId
	 * @param partitionid
	 * @param _reader
	 */
	public ConsumerReplica(ConsumerServer server,
			Map<Integer, ConnectionInfo> connections, int partitionid,
			DynamicBrokersReader reader, Configuration conf) {
		_partition = partitionid;
		_conf = conf;
		_reader = reader;
		path = committedPath(); // 存储offset的zk路径
		_consumer = connections.get(partitionid).consumer; // 获取SimpleConsumer
		try {
			_curoffset = _reader.fetchOffset(path);// 获取zk上面保存的offset
			if (_curoffset != null) {
				currentOffset = Long.parseLong(_curoffset); // 把zk上保存的offset写入本地缓存
			} else {
				// 如果zk上不存在offset, 则使用此partition最早的offset
				currentOffset = KafkaUtils.getOffset(_consumer,
						_conf.getTopics(), _partition,
						_conf.getStartOffsetTime());
			}
			System.out.println(("Read partition offset from: " + path
					+ "  --> " + currentOffset));
		} catch (Throwable e) {
			System.out.println("Error reading and/or parsing at ZkNode: "
					+ path + "\n" + e.getMessage());
			for (int triesnum = 1; triesnum < 4; triesnum++) {
				triesnum += 1;
				try {
					Thread.sleep(5000);
					_curoffset = _reader.fetchOffset(path);// 获取zk上面保存的offset
					if (_curoffset != null) {
						currentOffset = Long.parseLong(_curoffset); // 把zk上保存的offset写入本地缓存
					} else {
						// 如果zk上不存在offset, 则使用此partition最早的offset
						currentOffset = KafkaUtils.getOffset(_consumer,
								_conf.getTopics(), _partition,
								_conf.getStartOffsetTime());
					}
				} catch (Throwable ex) {
					// 尝试3次失败, 进行重新初始化
					if (triesnum == 4) {
						server.shutdown();
					}
					System.out
							.println("Retry reading and/or parsing at ZkNode: "
									+ path + " for " + triesnum + " times \n");
				}
			}
		}

		long earliestOffset = KafkaUtils.getOffset(_consumer,
				_conf.getTopics(), _partition, _conf.getStartOffsetTime());// ConsumerConfig.startOffsetTime
		/**
		 * 如果从zk中读取到的currentOffset比partition中的earlyOffset小, 则进行置换
		 */
		if (currentOffset - earliestOffset < 0 || currentOffset < 0) {
			currentOffset = earliestOffset;
			System.out.println("Starting Kafka " + _consumer.host() + ":"
					+ partitionid + " from offset " + currentOffset);
		}
		// producer = new KafkaProducer();
		producer = new SyncProducer();
		producer.init();
		_committedTo = currentOffset;
		list = new ArrayList<kafka.producer.KeyedMessage<String, Message>>();
		refreshMillis = _conf.getRefreshFreqSecs() * 1000L;
	}

	/**
	 * 从kafka上消费数据
	 */
	private void fill() {
		long currTime = System.currentTimeMillis();// 当前时间毫秒数
		_committedTo = currentOffset;
		// 按60秒对offset进行更新
		if (currTime > lastRefreshTimeMs + refreshMillis) {
			// 把offset更新到zookeeper
			_reader.writeBytes(path, (_committedTo + "").getBytes());
			lastRefreshTimeMs = currTime;
		}
		_committedTo = currentOffset;
		ByteBufferMessageSet msgs = null;
		try {
			msgs = KafkaUtils.fetchMessages(_conf, _consumer, _partition,
					_committedTo);
		} catch (UpdateOffsetException e) {
			_reader.writeBytes(path, (_committedTo + "").getBytes());
			System.out.println("Using new offset: {}" + _emittedToOffset);
			return;
		}
		if (msgs != null) {
			// 遍历读取到的消息集
			for (MessageAndOffset msg : msgs) {
				final Long cur_offset = msg.offset();// 当前消息的offset偏移量
				_waitingToEmit.add(msg); // 把数据放入队列中
				currentOffset += 1;// 把当前offset存入本地
			}
		}

		System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
	}

	/**
	 * producer发送数据
	 * 
	 * @param oList
	 */
	public void sendData(
			List<kafka.producer.KeyedMessage<String, Message>> oList) {
		try {
			producer.send(list);
			list.clear();
		} catch (Exception e) {
			System.out.println("Kafka Producer send Error!");
			try {
				Thread.sleep(500);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
	}

	/**
	 * zk上存储offset数据的路径
	 * 
	 * @return
	 */
	private String committedPath() {
		return "/consumers/demo/offsets/" + _conf.getTopics() + "/"
				+ _partition;
	}

	public void run() {

		int numMessages = 0;
		System.out.println("###############################");
		while (ConsumerServer.isSync) {
			if (_waitingToEmit.isEmpty()) {
				System.out.println("*********************************");
				fill();// 开始读取message
			}
			while (true) {// numMessages <= ProducerConf.BATCH_NUM_VALUE
				MessageAndOffset toEmit = _waitingToEmit.pollFirst();// 每次读取一条
				if (toEmit == null) {
					if (list.size() > 0) {
						sendData(list);
					}
					System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
					break;
				}
				numMessages += 1;
				System.out.println(new String(Utils.toByteArray(toEmit
						.message().payload())));
				KeyedMessage<String, Message> data = new KeyedMessage<String, Message>(
						_conf.getTargetTopic(), toEmit.message());
				list.add(data);

				if (list.size() == ProducerConf.BATCH_NUM_VALUE) {
					sendData(list);
					numMessages = 0;
				}
			}
		}
	}

}
