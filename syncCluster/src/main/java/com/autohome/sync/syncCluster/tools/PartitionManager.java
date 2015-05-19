package com.autohome.sync.syncCluster.tools;

import java.util.LinkedList;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;




























import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;


public class PartitionManager {

	long _emittedToOffset;
	SortedSet<Long> _pending = new TreeSet<Long>();
	SortedSet<Long> failed = new TreeSet<Long>();
	Long _committedTo;	//已经写入zk的offset
	String _topologyInstanceId;
	SpoutConfig _spoutConfig;
	LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
	Partition _partition;
	SimpleConsumer _consumer;
	DynamicPartitionConnections _connections;
	ZKState _state;
	Map _stromConf;
	
	
	
	public PartitionManager(DynamicPartitionConnections connections,String topologyInstanceId, ZKState state, Map stormConf, Partition id, SpoutConfig spoutConfig){
		_partition = id;
		_connections = connections;
		_topologyInstanceId = topologyInstanceId;
		_consumer = connections.register(id.host,id.partition);
		_state = state;
		 _spoutConfig = spoutConfig;
		
		String jsonTopologyId = null;
		Long jsonOffset = null;
		String path = committedPath();
		try {
			Map<Object,Object> json = _state.readJSON(path);
			System.out.println("Read partition information from: " + path +  "  --> " + json );
	        jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
	        jsonOffset = (Long) json.get("offset");// 从zk中读出commited offset
		 } catch (Throwable e) {
	            System.out.println("Error reading and/or parsing at ZkNode: " + path+"\n"+e.getMessage());
	     }
		
		//根据用户设置的startOffsetTime，值来读取offset（-2 从kafka头开始  -1 是从最新的开始 0 =无 从ZK开始）
		Long currentOffset = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig.startOffsetTime);
		 if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
        	 // zk中没有记录，那么根据spoutConfig.startOffsetTime设置offset，Earliest或Latest
            _committedTo = currentOffset;
            System.out.println("No partition information found, using configuration to determine offset");
        } else if (!topologyInstanceId.equals(jsonTopologyId) && spoutConfig.forceFromStart) {
            _committedTo = KafkaUtils.getOffset(_consumer, spoutConfig.topic, id.partition, spoutConfig.startOffsetTime);
            System.out.println("Topology change detected and reset from start forced, using configuration to determine offset");
        } else {
            _committedTo = jsonOffset;
            System.out.println("Read last commit offset from zookeeper: " + _committedTo + "; old topology_id: " + jsonTopologyId + " - new topology_id: " + topologyInstanceId );
        }
		 
		 /*
	         * 下面这个if判断是如果当前读取的offset值与提交到zk的值不一致, 且相差Long.MAX_VALUE, 就认为中间很大部分msg发射了没有提交, 
	         * 就把这部分全部放弃, 避免重发
	         * 令_committedTo = currentOffset, 这个是新修复的bug, 之前maxOffsetBehind=10000(这个值太小) bug-isuue STORM-399
	         */
	        if (currentOffset - _committedTo > spoutConfig.maxOffsetBehind || _committedTo <= 0) {
	            System.out.println("Last commit offset from zookeeper: " + _committedTo);
	            _committedTo = currentOffset;// 初始化时，中间状态都是一致的
	            System.out.println("Commit offset " + _committedTo + " is more than " +
	                    spoutConfig.maxOffsetBehind + " behind, resetting to startOffsetTime=" + spoutConfig.startOffsetTime);
	        }

	        System.out.println("Starting Kafka " + _consumer.host() + ":" + id.partition + " from offset " + _committedTo);

	}


	public EmitState next() {
		if(_waitingToEmit.isEmpty()){
			fill();//开始读取message
		}
		while(true){
			MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();//每次读取一条
			if(toEmit == null){
				return EmitState.NO_EMITTED;
			}
			if (!_waitingToEmit.isEmpty()) {
	            return EmitState.EMITTED_MORE_LEFT;
	        } else {
	            return EmitState.EMITTED_END;
	        }
		}
	}

	
	
	
	
	
	private void fill() {
		long start = System.nanoTime();
		long offset;
	 
		//首先要判断是否有fail的offset, 如果有的话, 在需要从这个offset开始往下去读取message,所以这里有重发的可能.
		boolean had_failed = !failed.isEmpty();
		if(had_failed){
			offset = failed.first();
		}else{
			offset = _emittedToOffset;
		}
		ByteBufferMessageSet msgs = null;
		try{
			msgs = KafkaUtils.fetchMessages(_spoutConfig, _consumer, _partition, offset);
		 } catch (UpdateOffsetException e) {//UpdateOffsetException e
			 _emittedToOffset = KafkaUtils.getOffset(_consumer, _spoutConfig.topic, _partition.partition, _spoutConfig);
			 System.out.println("Using new offset: {}"+"\n"+_emittedToOffset);
			 return;
		 }
		
		long end = System.nanoTime();
        long millis = (end - start) / 1000000;
        if(msgs != null){
        	int numMessages = 0;
        	for(MessageAndOffset msg : msgs){
        		Long cur_offset = msg.offset();
        		if(cur_offset < offset){
        			continue;
        		}
        		
        		if (!had_failed || failed.contains(cur_offset)) {
                    numMessages += 1;
                    _pending.add(cur_offset);
                    _waitingToEmit.add(new MessageAndRealOffset(msg.message(), cur_offset));
                    _emittedToOffset = Math.max(msg.nextOffset(), _emittedToOffset);
                    if (had_failed) {
                    	System.out.println(cur_offset);
                        failed.remove(cur_offset);// 如果失败列表中含有该offset，就移除，因为要重新发射了。
                    }
                }
            }
        	}
     }


	/**
	 * 提交地址
	 * @return
	 */
	private String committedPath() {
		
		return null;
	}
	
	 public void close() {
	        _connections.unregister(_partition.host, _partition.partition);
	 }
	 
	 static class KafkaMessageId {
	        public Partition partition;
	        public long offset;

	        public KafkaMessageId(Partition partition, long offset) {
	            this.partition = partition;
	            this.offset = offset;
	        }
	    }
}



