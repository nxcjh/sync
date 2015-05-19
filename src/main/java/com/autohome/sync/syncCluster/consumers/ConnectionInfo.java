package com.autohome.sync.syncCluster.consumers;



import kafka.javaapi.consumer.SimpleConsumer;

public class ConnectionInfo {

	  SimpleConsumer consumer;
      Integer partitions;
      
	public ConnectionInfo(Integer partitions,SimpleConsumer consumer) {
		this.consumer = consumer;
		this.partitions = partitions;
	}
      
      
      
      
}
