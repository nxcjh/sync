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

import kafka.javaapi.consumer.SimpleConsumer;

public class ConsumerServer {

	DynamicBrokersReader _reader; //操作zookeeper信息类
	int numPartitions ;	//获得指定topic的所有分区数
	String hostName;	//本机主机名
	String brokerId;	//当前节点的brokerid
	Set<Integer> partitions;	//以当前节点为leader的分区
	ExecutorService executor;	//线程池
	Map<Integer,ConnectionInfo> conMap = new HashMap<Integer,ConnectionInfo>();	//[partitionid, [partitionid,SimpleConsumer]]
	
	
	/**
	 * 程序启动初始化类
	 * @throws UnknownHostException
	 */
	public void open() throws UnknownHostException{
		
		_reader = new DynamicBrokersReader(ConsumerConfig.zkHosts,ConsumerConfig.topics);
		numPartitions = _reader.getNumPartitions();
		hostName = InetAddress.getLocalHost().getHostName();
//		brokerId = _reader.getBrokerId(hostName);
		brokerId = _reader.getBrokerId("node2.auto.com");
		partitions = _reader.getPartitions(numPartitions,Integer.parseInt(brokerId));
		List<String> zkHosts = toList(ConsumerConfig.zkHosts);
		String clientId = null;
		for(Integer i : partitions){
			clientId = "Client_" + ConsumerConfig.clientId+"_"+i;
			conMap.put(i, new ConnectionInfo(i,new SimpleConsumer(hostName,9092,ConsumerConfig.socketTimeoutMs,ConsumerConfig.bufferSizeBytes,clientId)));
		}
		executor = Executors.newFixedThreadPool(partitions.size());
	}
	
	
	public  void start(){
//		for(Integer i : partitions){
//			Runnable runner = new ConsumerReplica(conMap,ConsumerConfig.topologyInstanceId,i,_reader);
//			System.out.println(ConsumerConfig.topics+": "+ConsumerConfig.topologyInstanceId+": partition="+i+" is running....");
//			executor.execute(runner);
//		}
		Runnable runner = new ConsumerReplica(conMap,ConsumerConfig.topologyInstanceId,0,_reader);
		runner.run();
	}
	
	
	
	 private List<String> toList(String zkHosts) {
		String[] hostStrs = zkHosts.split(",");
		return Arrays.asList(hostStrs);
	}

	 public void shutdown(){
		 _reader.close();
	 }


	public static void main(String args[]) throws UnknownHostException {
		ConsumerServer server  = new ConsumerServer();
		server.open();
		server.start();
		server.shutdown();
		
	}
}
