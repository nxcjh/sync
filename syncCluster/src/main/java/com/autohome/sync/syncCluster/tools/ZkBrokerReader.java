package com.autohome.sync.syncCluster.tools;

import java.net.SocketTimeoutException;
import java.util.Map;





public class ZkBrokerReader implements IBrokerReader {

	GlobalPartitionInformation cachedBrokers;
	DynamicBrokersReader reader;
	long lastRefreshTimeMs;
	long refreshMillis;
	
	public ZkBrokerReader(Map conf, String topic, ZKHosts hosts){
		try {
			reader = new DynamicBrokersReader(hosts.brokerZkStr,topic);
			cachedBrokers = reader.getBrokerInfo();
			lastRefreshTimeMs = System.currentTimeMillis();
			refreshMillis = hosts.refreshFreqSecs * 1000L;
		} catch (java.net.SocketTimeoutException e) {
			System.out.println("Failed to update brokers :\n"+e);
		}
		
		
	}
	
	public GlobalPartitionInformation getCurrentBrokers() {
		long currTime = System.currentTimeMillis();
		if (currTime > lastRefreshTimeMs + refreshMillis) {
			try {
				System.out.println("brokers need refreshing because " + refreshMillis + "ms have expired");
				cachedBrokers = reader.getBrokerInfo();
				lastRefreshTimeMs = currTime;
			} catch (java.net.SocketTimeoutException e) {
				System.out.println("Failed to update brokers:\n"+ e.getMessage());
			}
		}
		return cachedBrokers;
	}

	
	public void close() {
		reader.close();
	}
	

	
}
