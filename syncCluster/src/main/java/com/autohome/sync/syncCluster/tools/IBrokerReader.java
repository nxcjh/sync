package com.autohome.sync.syncCluster.tools;

public interface IBrokerReader {

	GlobalPartitionInformation getCurrentBrokers();
	
	void close();
}
