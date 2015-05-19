package com.autohome.sync.syncCluster;
import java.util.HashMap;
import java.util.List;
import java.util.Map;







import com.autohome.sync.syncCluster.tools.DynamicPartitionConnections;
import com.autohome.sync.syncCluster.tools.KafkaUtils;
import com.autohome.sync.syncCluster.tools.PartitionCoordinator;
import com.autohome.sync.syncCluster.tools.SpoutConfig;
import com.autohome.sync.syncCluster.tools.ZKState;
import com.autohome.sync.syncCluster.tools.ZkCoordinator;




/**
 * 程序运行主类
 * @author nxcjh
 *
 */
public class SyncServer {
	
	SpoutConfig _spoutConfig;
	PartitionCoordinator _coordinator;
    DynamicPartitionConnections _connections;
    ZKState _state;
	
	


	public void open(Map conf){
		 Map stateConf = new HashMap(conf);
		 List<String> zkServers = _spoutConfig.zkServers; //zk的ip地址
		 Integer zkPort = _spoutConfig.zkPort;//zk的port
		 stateConf.put("zookeeper.servers", zkServers);
	     stateConf.put("zookeeper.port", zkPort);
	     stateConf.put("zookeeper.rootpath", _spoutConfig.zkRoot);
		 _state = new ZKState(stateConf);
 
		 _connections = new DynamicPartitionConnections(_spoutConfig, KafkaUtils.makeBrokerReader(conf, _spoutConfig));
//		 _coordinator = new ZkCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, _uuid);
	}
	
	
	
	public static void main(String[] args) {
		SyncServer server = new SyncServer();
		
	}
	

	
}
