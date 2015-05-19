package com.autohome.sync.syncCluster.tools;

import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;





public class DynamicBrokersReader {

	
	
	 private CuratorFramework _curator;
	    private String _zkPath;
	    private String _topic;
	    private final static int STORM_ZOOKEEPER_SESSION_TIMEOUT = 10000;
	    private final static int STORM_ZOOKEEPER_RETRY_TIMES = 10;
	    private final static int STORM_ZOOKEEPER_RETRY_INTERVAL = 10000;
	    /**
	     * 获取zk连接
	     * @param zkStr
	     * @param topic
	     */
	    public DynamicBrokersReader(String zkStr, String topic) {
	        _topic = topic;
	        try {
	            _curator = CuratorFrameworkFactory.newClient(
	                    zkStr,
	                    STORM_ZOOKEEPER_SESSION_TIMEOUT,
	                    15000,
	                    new RetryNTimes(STORM_ZOOKEEPER_RETRY_TIMES,STORM_ZOOKEEPER_RETRY_INTERVAL));
	            _curator.start();
	        } catch (Exception ex) {
	            System.err.println("Couldn't connect to zookeeper: \n"+ex);
	        }
	    }

	    /**
	     * 获取所有的分区
	     * @return
	     */
	    private int getNumPartitions() {
	        try {
	            String topicBrokersPath = partitionPath();
	            List<String> children = _curator.getChildren().forPath(topicBrokersPath);
	            return children.size();
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }
	    }
	    
	    /**
	     * 获取指定topic的所有partition的leader信息
	     * Get all partitions with their current leaders
	     */
	    public GlobalPartitionInformation getBrokerInfo() throws SocketTimeoutException {
	      GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
	        try {
	            int numPartitionsForTopic = getNumPartitions();
	            String brokerInfoPath = brokerPath();
	            for (int partition = 0; partition < numPartitionsForTopic; partition++) {
	                int leader = getLeaderFor(partition);
	                String path = brokerInfoPath + "/" + leader;
	                try {
	                    byte[] brokerData = _curator.getData().forPath(path);
	                    Broker hp = getBrokerHost(brokerData);
	                    globalPartitionInformation.addPartition(partition, hp);
	                } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
	                    System.err.println("Node {} does not exist \n"+path);
	                }
	            }
	        } catch (SocketTimeoutException e) {
						throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }
	       System.err.println("Read partition info from zookeeper: " + globalPartitionInformation);
	        return globalPartitionInformation;
	    }
	    
	    
	    /**
	     * [zk: localhost:2181(CONNECTED) 56] get /brokers/ids/0
	     * { "host":"localhost", "jmx_port":9999, "port":9092, "version":1 }
	     *
	     * @param contents
	     * @return
	     */
	    private Broker getBrokerHost(byte[] contents) {
	        try {
	            Map<Object, Object> value = (Map<Object, Object>) JSONValue.parse(new String(contents, "UTF-8"));
	            String host = (String) value.get("host");
	            Integer port = ((Long) value.get("port")).intValue();
	            return new Broker(host, port);
	        } catch (UnsupportedEncodingException e) {
	            throw new RuntimeException(e);
	        }
	    }
	    
	    public void close() {
	        _curator.close();
	    }
	    
	  public String partitionPath() {
	        return  "/brokers/topics/" + _topic + "/partitions";
	    }

	    public String brokerPath() {
	        return  "/brokers/ids";
	    }
	    
	    /**
	     * 
	     * 获取指定分区的laderid
	     * get /brokers/topics/distributedTopic/partitions/1/state
	     * { "controller_epoch":4, "isr":[ 1, 0 ], "leader":1, "leader_epoch":1, "version":1 }
	     *
	     * @param partition
	     * @return
	     */
	    private int getLeaderFor(long partition) {
	        try {
	            String topicBrokersPath = partitionPath();
	            byte[] hostPortData = _curator.getData().forPath(topicBrokersPath + "/" + partition + "/state");
	            Map<Object, Object> value = (Map<Object, Object>) JSONValue.parse(new String(hostPortData, "UTF-8"));
	            Integer leader = ((Number) value.get("leader")).intValue();
	            return leader;
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }
	    }
	    
	    
	    /**
	     * 读取节点信息
	     * @param path
	     * @return
	     */
	    public byte[] readBytes(String path) {
	        try {
	            if (_curator.checkExists().forPath(path) != null) {
	                return _curator.getData().forPath(path);
	            } else {
	                return null;
	            }
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }
	    }
	    
	    /**
	     * 读取节点信息, 返回json串
	     * @param path
	     * @return
	     */
	    public Map<Object, Object> readJSON(String path) {
	        try {
	            byte[] b = readBytes(path);
	            if (b == null) {
	                return null;
	            }
	            return (Map<Object, Object>) JSONValue.parse(new String(b, "UTF-8"));
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }
	    }
	    
	    
	    
	    public void writeBytes(String path, byte[] bytes) {
	        try {
	            if (_curator.checkExists().forPath(path) == null) {
	                _curator.create()
	                        .creatingParentsIfNeeded()
	                        .withMode(CreateMode.PERSISTENT)
	                        .forPath(path, bytes);
	            } else {
	                _curator.setData().forPath(path, bytes);
	            }
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }
	    }

	    /**
	     * 向节点下写入信息
	     * @param path
	     * @param data
	     */
	    public void writeJSON(String path, Map<Object, Object> data) {
	       System.out.println("Writing " + path + " the data " + data.toString());
	        writeBytes(path, JSONValue.toJSONString(data).getBytes(Charset.forName("UTF-8")));
	    }
	    
	    public static void main(String[] args) throws SocketTimeoutException {
	    	DynamicBrokersReader zkc = new DynamicBrokersReader("10.168.100.182:2181","adserver");
	    	System.out.println(zkc.getNumPartitions());
	    	GlobalPartitionInformation info = zkc.getBrokerInfo();
	    	System.out.println(info.getBrokerFor(1));
//	    	Iterator<Partition> it = info.iterator();
//	    	while(it.hasNext()){
//	    		Partition p = it.next();
//	    		System.out.println("=="+p.getId()+"\t"+p.host+"\t"+p.partition);
//	    	}
	    	
	    	Map<Object, Object> map = zkc.readJSON("/brokers/topics/adserver/partitions/1/state");
	    	System.out.println(map.toString());
	    	zkc.close();
		}
}
