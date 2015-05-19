package com.autohome.sync.syncCluster.consumers;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;






import com.autohome.sync.syncCluster.tools.DynamicPartitionConnections;
import com.autohome.sync.syncCluster.tools.MessageAndRealOffset;
import com.autohome.sync.syncCluster.tools.ZKState;

import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;


public class ConsumerReplica implements Runnable{

	Long _emittedToOffset;	//从kafka读到的offset, 从kafka读到的messages会放入_waitingToEmit,放入这个list, 我们就认为一定会被emit, 所以emittedToOffset可以认为是从kafka读到的offset.
    SortedSet<Long> _pending = new TreeSet<Long>();
    SortedSet<Long> failed = new TreeSet<Long>();
    Long _committedTo;	//已经写入zk的offset
    LinkedList<MessageAndRealOffset> _waitingToEmit = new LinkedList<MessageAndRealOffset>();
    int _partition;
   
    String _topologyInstanceId;
    SimpleConsumer _consumer;
    DynamicPartitionConnections _connections;
    ZKState _state;
    Map _stormConf;
    
    
	public ConsumerReplica(Map<Integer,ConnectionInfo> connections,String topologyInstanceId,int partitionid,DynamicBrokersReader _reader) {
		 _partition = partitionid;
		 String jsonTopologyId = null;
	     Long jsonOffset = null;
		 String path = committedPath();
		 _consumer = connections.get(partitionid).consumer;
		 try {
			 Map<Object, Object> json = _reader.readJSON(path);
			 System.out.println(("Read partition information from: " + path +  "  --> " + json ));
			 if (json != null) {
	             jsonTopologyId = (String) ((Map<Object, Object>) json.get("topology")).get("id");
	             jsonOffset = (Long) json.get("offset");// 从zk中读出commited offset
	         }
		  } catch (Throwable e) {
	            System.out.println("Error reading and/or parsing at ZkNode: " + path+"\n"+e.getMessage());
	      }
		 
		 Long currentOffset = KafkaUtils.getOffset(_consumer, ConsumerConfig.topics, partitionid);
		 
		 if (jsonTopologyId == null || jsonOffset == null) { // failed to parse JSON?
        	 // zk中没有记录，那么根据spoutConfig.startOffsetTime设置offset，Earliest或Latest
            _committedTo = currentOffset;
            System.out.println("No partition information found, using configuration to determine offset");
        } else if (!topologyInstanceId.equals(jsonTopologyId) && ConsumerConfig.forceFromStart) {
            _committedTo = KafkaUtils.getOffset(_consumer, ConsumerConfig.topics, partitionid, ConsumerConfig.startOffsetTime);
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
	        if (currentOffset - _committedTo > ConsumerConfig.maxOffsetBehind || _committedTo <= 0) {
	             System.out.println("Last commit offset from zookeeper: " + _committedTo);
	            _committedTo = currentOffset;// 初始化时，中间状态都是一致的
	             System.out.println("Commit offset " + _committedTo + " is more than " +
	                    ConsumerConfig.maxOffsetBehind + " behind, resetting to startOffsetTime=" + ConsumerConfig.startOffsetTime);
	        }
	         System.out.println("Starting Kafka " + _consumer.host() + ":" + partitionid + " from offset " + _committedTo);
	        _emittedToOffset = _committedTo;
	}
    
    
	private void fill() {
        long start = System.nanoTime();
        long offset;
        //首先要判断是否有fail的offset, 如果有的话, 在需要从这个offset开始往下去读取message,所以这里有重发的可能.
        final boolean had_failed = !failed.isEmpty();

        // Are there failed tuples? If so, fetch those first.
        if (had_failed) {
            offset = failed.first();
        } else {
            offset = _emittedToOffset;//取失败的最小的offset值
        }

        ByteBufferMessageSet msgs = null;
        try {
            msgs = KafkaUtils.fetchMessages( _consumer, _partition, offset);
            
        } catch (Exception e) {//UpdateOffsetException e
            _emittedToOffset = KafkaUtils.getOffset(_consumer, ConsumerConfig.topics, _partition);
            System.out.println("Using new offset: {}"+ _emittedToOffset);
            // fetch failed, so don't update the metrics
            return;
        }
        
        long end = System.nanoTime();
        long millis = (end - start) / 1000000;
        if (msgs != null) {
            int numMessages = 0;

            for (MessageAndOffset msg : msgs) {
                final Long cur_offset = msg.offset();
                if (cur_offset < offset) {
                    // Skip any old offsets.
                    continue;
                }
                     
                /**
                 * 只要是没有失败的或者失败的set中含有该offset(因为失败msg有很多,我们只是从最小的offset开始读取msg的),就把这个message放到带发射的list中
                 */
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
    
	
	public void next(){
		while(true){
			 if (_waitingToEmit.isEmpty()) {
		            fill();//开始读取message
		        }
			 while (true) {
		            MessageAndRealOffset toEmit = _waitingToEmit.pollFirst();//每次读取一条
		            if (toEmit == null) {
		               continue;
		            }		            
		            //发送数据
		            //可以看看转换tuple的过程， 可以看到是通过kafkaConfig.scheme.deserialize来做转换
//		            Iterable<List<Object>> tups = KafkaUtils.generateTuples(_spoutConfig, toEmit.msg);
//		            if (tups != null) {
//		                for (List<Object> tup : tups) {
//		                    collector.emit(tup, new KafkaMessageId(_partition, toEmit.offset));
//		                }
//		                break;//这里就是没成功发射一条msg, 就break掉, 返回emitstate给kafkaSpout的nextTuple中做判断和定时commit成功处理的offset到zk
//		            } else {
////		                ack(toEmit.offset);//ack 做清楚工作
//		            }
		        }
		}
		
	}
    
	private String committedPath() {
        return "";
    }


	@Override
	public void run() {
		next();
		
	}
    
    
}
