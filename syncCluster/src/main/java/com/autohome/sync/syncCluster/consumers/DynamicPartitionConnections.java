package com.autohome.sync.syncCluster.consumers;

import java.util.HashMap;
import java.util.Map;



import kafka.javaapi.consumer.SimpleConsumer;




public class DynamicPartitionConnections {

	Map<Integer, ConnectionInfo> _connections = new HashMap();
	
	
	
	public DynamicPartitionConnections(Map<Integer,ConnectionInfo> connections) {
	
		this._connections = connections;
	}



//	public SimpleConsumer register(Broker host,int partition,Map<Integer, ConnectionInfo> _connections) {
//        if(!_connections.containsKey(partition)){
//        	_connections.put(partition,  new ConnectionInfo(partition,new SimpleConsumer(host.host, host.port, ConsumerConfig.socketTimeoutMs, ConsumerConfig.bufferSizeBytes, ConsumerConfig.clientId)));
//        }
//        ConnectionInfo info = _connections.get(partition);
//        return info.consumer;
//    }
}
