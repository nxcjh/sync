package com.autohome.sync.syncCluster.consumers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.consumer.SimpleConsumer;

public class ConsumerServer {

	DynamicBrokersReader _reader; // 操作zookeeper信息类
	int numPartitions; // 获得指定topic的所有分区数
	String hostName; // 本机主机名
	String brokerId; // 当前节点的brokerid
	Set<Integer> partitions; // 以当前节点为leader的分区
	ExecutorService executor; // 线程池
    Set<Integer> t_partitionSet;
	Map<Integer, ConnectionInfo> conMap = new HashMap<Integer, ConnectionInfo>(); // [partitionid,																				// [partitionid,SimpleConsumer]]
	public static ScheduledExecutorService scheduledExecutorService = Executors
			.newScheduledThreadPool(1);	//定时对比本地与zookeeper状态

	/**
	 * 程序启动初始化类
	 * 
	 * @throws UnknownHostException
	 */
	public void open() throws UnknownHostException {
		getBrokerInfo();
		List<String> zkHosts = toList(ConsumerConfig.zkHosts);
		String clientId = null;
		for (Integer i : partitions) {
			clientId = KafkaUtils.getClientId(ConsumerConfig.clientId, i+"");
			conMap.put(i, new ConnectionInfo(i, new SimpleConsumer(hostName,
					9092, ConsumerConfig.socketTimeoutMs,
					ConsumerConfig.bufferSizeBytes, clientId)));
		}
		executor = Executors.newFixedThreadPool(partitions.size());
	}

	/**
	 * 获取当前节点上的leader所在partition分区的集合
	 * @return
	 */
	public Set<Integer> getBrokerInfo() {
		try {
			_reader = new DynamicBrokersReader(ConsumerConfig.zkHosts,
					ConsumerConfig.topics);
			numPartitions = _reader.getNumPartitions();
			hostName = InetAddress.getLocalHost().getHostName();
			// brokerId = _reader.getBrokerId(hostName);
			hostName = "node2.auto.com";
			brokerId = _reader.getBrokerId("node2.auto.com");
			partitions = _reader.getPartitions(numPartitions,
					Integer.parseInt(brokerId));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return partitions;
	}

	/**
	 * 程序启动主函数
	 */
	public void start() {
//		for (Integer i : partitions) {
//			Runnable runner = new ConsumerReplica(conMap,
//					ConsumerConfig.topologyInstanceId, i, _reader);
//			System.out.println(ConsumerConfig.topics + ": "
//					+ ConsumerConfig.topologyInstanceId + ": partition=" + i
//					+ " is running....");
//			executor.execute(runner);
//		}
		
		
		executor.execute(new ConsumerReplica(conMap,ConsumerConfig.topologyInstanceId,0,_reader));
		scheduledExecutorService.scheduleAtFixedRate(new SchedulerTask(), 0,30, TimeUnit.SECONDS);

	}

	

	private List<String> toList(String zkHosts) {
		String[] hostStrs = zkHosts.split(",");
		return Arrays.asList(hostStrs);
	}

	public void shutdown() {
		_reader.close();
	}

	
	/**
	 * 对比zookeeper中数据与本地数据的一致性
	 * @author nxcjh
	 *
	 */
	private class SchedulerTask implements Runnable {

		public void run() {
			t_partitionSet = getBrokerInfo();
			if (t_partitionSet.size() != partitions.size()) {
				// 进行重新缓存zk数据
			}
			for (Integer i : t_partitionSet) {
				if (!partitions.contains(i)) {
					// 进行重新缓存zk数据
				}
			}
		}
	}

	public static void main(String args[]) throws UnknownHostException {
		ConsumerServer server = new ConsumerServer();
		server.open();
		server.start();
		server.shutdown();

	}
	
}


