package com.autohome.sync.syncCluster.consumers;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.autohome.sync.syncCluster.tools.Configuration;

import kafka.javaapi.consumer.SimpleConsumer;

/**
 * 程序运行的主类
 * 
 * @author nxcjh
 *
 */
public class ConsumerServer {

	DynamicBrokersReader _reader; // 操作zookeeper信息类
	int numPartitions; // 获得指定topic的所有分区数
	String hostName; // 本机主机名
	String brokerId; // 当前节点的brokerid
	Set<Integer> partitions; // 以当前节点为leader的分区
	ExecutorService executor; // 线程池
	Set<Integer> t_partitionSet;
	Configuration conf = new Configuration();
	public static boolean isSync = true;
	public static String confPath;
	Map<Integer, ConnectionInfo> conMap = new HashMap<Integer, ConnectionInfo>(); // [partitionid,
																					// //
																					// [partitionid,SimpleConsumer]]
	public static ScheduledExecutorService scheduledExecutorService = Executors
			.newScheduledThreadPool(1); // 定时对比本地与zookeeper状态

	/**
	 * 程序启动初始化类
	 * 
	 * @throws UnknownHostException
	 */
	public void open() throws UnknownHostException {
		loadProperties(confPath);
		getBrokerInfo();
		List<String> zkHosts = toList(conf.getZkHosts());

		for (Integer i : partitions) {
			conMap.put(i, new ConnectionInfo(i, new SimpleConsumer(hostName,
					9092, conf.getSocketTimeoutMs(), conf.getBufferSizeBytes(),
					KafkaUtils.getClientId(conf.getClientId(), i + ""))));
		}
		executor = Executors.newFixedThreadPool(partitions.size());
	}

	/**
	 * 获取当前节点上的leader所在partition分区的集合
	 * 
	 * @return
	 */
	public Set<Integer> getBrokerInfo() {
		try {
			_reader = new DynamicBrokersReader(conf.getZkHosts(),
					conf.getTopics());
			numPartitions = _reader.getNumPartitions();
			hostName = InetAddress.getLocalHost().getHostName();
			brokerId = _reader.getBrokerId(hostName);
//			hostName = "node2.auto.com"; // test
//			brokerId = _reader.getBrokerId("node2.auto.com");
			partitions = _reader.getPartitions(numPartitions,
					Integer.parseInt(brokerId));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return partitions;
	}

	/**
	 * 程序启动主函数
	 * 
	 * @param server
	 */
	public void start(ConsumerServer server) {
		for (Integer i : partitions) {
			Runnable runner = new ConsumerReplica(this, conMap, i, _reader,
					conf);
			System.out.println(conf.getTopics() + ":  partition=" + i
					+ " is running....");
			executor.execute(runner);
		}

		// executor.execute(new ConsumerReplica(server,conMap,0,_reader,conf));
		scheduledExecutorService.scheduleAtFixedRate(new SchedulerTask(), 0,
				30, TimeUnit.SECONDS);
		// scheduledExecutorService.scheduleAtFixedRate(new SchedulerTask2(),
		// 0,60, TimeUnit.SECONDS);
	}

	public void loadProperties(String filePath) {
		// 加载配置文件
		Properties prop = new Properties();
		InputStream in = null;
		try {
			in = new BufferedInputStream(new FileInputStream(filePath));
			prop.load(in);
			Enumeration enum1 = prop.propertyNames();
			;// 得到配置文件的名字
			while (enum1.hasMoreElements()) {
				String strKey = (String) enum1.nextElement();
				System.out.println("server.properties " + strKey + "="
						+ prop.getProperty(strKey));
			}
			// 设置配置属性
			conf.setTopics(prop.getProperty("sync.consumer.topic"));
			conf.setZkHosts(prop.getProperty("sync.consumer.zookeeper.hosts"));
			conf.setClientId(prop.getProperty("sync.consumer.client.id"));
			conf.setSocketTimeoutMs(Integer.parseInt(prop
					.getProperty("sync.consumer.socket.timeout.ms")));
			conf.setBufferSizeBytes(Integer.parseInt(prop
					.getProperty("sync.consumer.read.bufferszie.bytes")));
			// conf.setStartOffsetTime(Long.parseLong(prop.getProperty("sync.consumer.startoffsettime")));
			conf.setRefreshFreqSecs(Long.parseLong(prop
					.getProperty("sync.offsetupdate.refresh.sec")));
			conf.setTargetTopic(prop.getProperty("sync.producer.target.topic"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private List<String> toList(String zkHosts) {
		String[] hostStrs = zkHosts.split(",");
		return Arrays.asList(hostStrs);
	}

	public void shutdown() {
		_reader.close();
	}

	public void restart() {
		ConsumerServer.isSync = false;
		shutdown();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		ConsumerServer.isSync = true;
		try {
			open();
			start(this);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 对比zookeeper中数据与本地数据的一致性
	 * 
	 * @author nxcjh
	 *
	 */
	private class SchedulerTask implements Runnable {

		public void run() {
			t_partitionSet = getBrokerInfo();
			if (t_partitionSet.size() != partitions.size()) {
				// 进行重新缓存zk数据
				restart();

			}
			for (Integer i : t_partitionSet) {
				if (!partitions.contains(i)) {
					// 进行重新缓存zk数据
					restart();
				}
			}
		}
	}

	private class SchedulerTask2 implements Runnable {

		public void run() {
			try {
				Thread.sleep(20000);
				ConsumerServer.isSync = false;
				Thread.sleep(20000);
				restart();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String args[]) throws UnknownHostException {
		ConsumerServer server = new ConsumerServer();
		confPath = args[0];
		server.open();
		server.start(server);
		// server.shutdown();
	}

}
