package com.autohome.sync.syncCluster.consumers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.autohome.sync.syncCluster.producers.ProducerConf;
import com.autohome.sync.syncCluster.producers.SyncProducer;
import com.autohome.sync.syncCluster.tools.Configuration;
import com.autohome.sync.syncCluster.tools.UpdateOffsetException;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;

public class ConsumerReplica implements Runnable {
	private static final Logger LOG = Logger.getLogger(ConsumerReplica.class);
	private Long _emittedToOffset; // 从kafka读到的offset,
	// 从kafka读到的messages会放入_waitingToEmit,放入这个list,
	// 我们就认为一定会被emit,
	// 所以emittedToOffset可以认为是从kafka读到的offset.
	private long commitOffset;
	private LinkedList<MessageAndOffset> _waitingToEmit = new LinkedList<MessageAndOffset>();
	private int _partition;
	private String _curoffset;
	private Long currentOffset;
	private SimpleConsumer _consumer;
	private List<kafka.producer.KeyedMessage<String, String>> list = null;
	private SyncProducer producer;
	private long lastRefreshTimeMs;
	private long refreshMillis;
	private DynamicBrokersReader _reader;
	private String path;
	private Configuration _conf;
	private long payloadCount = 0;
	private long cacheTimeMillis = 0;

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
			LOG.info(("Read partition offset from: " + path
					+ "  --> " + currentOffset));
		} catch (Throwable e) {
			LOG.error("Error reading and/or parsing at ZkNode: "
					+ path + "\n" , e);
			
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
					LOG.info("Retry reading and/or parsing at ZkNode: "
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
			LOG.info("Starting Kafka " + _consumer.host() + ":"
					+ partitionid + " from offset " + currentOffset);
		}
		// producer = new KafkaProducer();
		producer = new SyncProducer();
		producer.init();
		list = new ArrayList<kafka.producer.KeyedMessage<String, String>>();
		refreshMillis = _conf.getRefreshFreqSecs() * 1000L;
	}

	/**
	 * 从kafka上消费数据
	 */
	private void fill() {
		commitOffset = currentOffset;
		ByteBufferMessageSet msgs = null;
		try {
			msgs = KafkaUtils.fetchMessages(_conf, _consumer, _partition,
					currentOffset);
		} catch (UpdateOffsetException e) {
			_reader.writeBytes(path, (currentOffset + "").getBytes());
			LOG.error("Using new offset: {}" + _emittedToOffset);
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
	}
	
	/**
	 * 限速
	 * @param currTime
	 */
	public void splitlimit(long currTime){
		if((currTime - cacheTimeMillis)/1000 >= 60){
			if(payloadCount/1024/1024 > 10){
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			payloadCount = 0l;
			cacheTimeMillis = currTime;
		}
	}

	/**
	 * producer发送数据
	 * 
	 * @param oList
	 */
	public void sendData(
			List<kafka.producer.KeyedMessage<String, String>> oList) {
		try {
			long currTime = System.currentTimeMillis();// 当前时间毫秒数
			splitlimit(currTime);
			producer.send(list);
			LOG.info(_conf.getTopics() +" : "+_partition+" producer send to :"+commitOffset +" msgs.");
			list.clear();
			
			// 按60秒对offset进行更新
			if (currTime > lastRefreshTimeMs + refreshMillis) {
				// 把offset更新到zookeeper
				_reader.writeBytes(path, (currentOffset + "").getBytes());
				lastRefreshTimeMs = currTime;
			}

		} catch (Exception e) {
			LOG.error("Kafka Producer send Error!");
			_reader.writeBytes(path, (currentOffset-list.size() + "").getBytes());
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
		int nullcount = 0;
		try{
		while (ConsumerServer.isSync) {
			if (_waitingToEmit.isEmpty()) {
				fill();// 开始读取message
			}
			while (true) {// numMessages <= ProducerConf.BATCH_NUM_VALUE
				MessageAndOffset toEmit = _waitingToEmit.pollFirst();// 每次读取一条
				if (toEmit == null) {
					
					if (list.size() > 0) {
						commitOffset += list.size();
						sendData(list);
					}else{
						if(nullcount > 10){
							LOG.error(_conf.getTopics() +" : "+_partition+" has :"+nullcount*_conf.getNodataInterval() +" mins no data to send.");
						}
						Thread.sleep(_conf.getNodataInterval()*1000*60);
						nullcount++;
					}
					break;
				}
				numMessages += 1;
//				LOG.info(new String(toByteArray(toEmit.message().payload())));
				
				
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(
						_conf.getTargetTopic(), numMessages +"",new String(toByteArray(toEmit.message().payload()),"UTF-8"));
				list.add(data);
				payloadCount += toEmit.message().payloadSize(); 
				if (list.size() == ProducerConf.BATCH_NUM_VALUE) {
					commitOffset += ProducerConf.BATCH_NUM_VALUE;
					sendData(list);
					numMessages = 0;
					
				}
			}
		}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	 public static byte[] toByteArray(ByteBuffer buffer) {
	        byte[] ret = new byte[buffer.remaining()];
	        buffer.get(ret, 0, ret.length);
	        return ret;
	 }

}
