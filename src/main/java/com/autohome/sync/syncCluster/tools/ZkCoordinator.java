package com.autohome.sync.syncCluster.tools;

import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;









public class ZkCoordinator  implements PartitionCoordinator {

	 SpoutConfig _spoutConfig;
	 String _topologyInstanceId;
	 Map<Partition, PartitionManager> _managers = new HashMap();
	 List<PartitionManager> _cachedList;
	 Long _lastRefreshTime = null;
	 int _refreshFreqMs;
	 DynamicPartitionConnections _connections;
	 DynamicBrokersReader _reader;
	 ZKState _state;
	 Map _stormConf;
	
	 public ZkCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig spoutConfig, ZKState state, String topologyInstanceId) {
		 this(connections, stormConf, spoutConfig, state,  topologyInstanceId, buildReader(stormConf, spoutConfig));
	 }
	 
	 public ZkCoordinator(DynamicPartitionConnections connections, Map stormConf, SpoutConfig spoutConfig, ZKState state,  String topologyInstanceId, DynamicBrokersReader reader) {
		 _spoutConfig = spoutConfig;
	     _connections = connections;
	     _topologyInstanceId = topologyInstanceId;
	     _stormConf = stormConf;
	     _state = state;
	     ZKHosts brokerConf = (ZKHosts) spoutConfig.hosts;
	     _refreshFreqMs = brokerConf.refreshFreqSecs * 1000;
	     _reader = reader;
	 }
	 
	 
	 private static DynamicBrokersReader buildReader(Map stormConf, SpoutConfig spoutConfig) {
	        ZKHosts hosts = (ZKHosts) spoutConfig.hosts;
	        return new DynamicBrokersReader(hosts.brokerZkStr,spoutConfig.topic);
	    }
	 
	 
	@Override
	public List<PartitionManager> getMyManagedPartitions() {
		 if (_lastRefreshTime == null || (System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
	            refresh();
	            _lastRefreshTime = System.currentTimeMillis();
	        }
	        return _cachedList;
	}

	private void refresh() {
		try {
			GlobalPartitionInformation brokerInfo = _reader.getBrokerInfo();
		} catch (SocketTimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Set<Partition> curr = _managers.keySet();
		
		
	}
	
	 public PartitionManager getManager(Partition partition) {
	        return _managers.get(partition);
	    }
	

}
