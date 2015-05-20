package com.autohome.sync.syncCluster.consumers;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.util.HashMap;
import java.util.Map;













import com.autohome.sync.syncCluster.tools.Configuration;
import com.autohome.sync.syncCluster.tools.FailedFetchException;
import com.autohome.sync.syncCluster.tools.KafkaError;
import com.autohome.sync.syncCluster.tools.UpdateOffsetException;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;

public class KafkaUtils {
	private static final int NO_OFFSET = -5;

	/**
	 * 获取zookeeper中保存的offset
	 * @param consumer
	 * @param topic
	 * @param partition
	 * @param whichTime
	 * @param clientName
	 * @return
	 */
	 public static long getOffset(Configuration conf , SimpleConsumer consumer, String topic, int partition) {
	        long startOffsetTime = kafka.api.OffsetRequest.LatestTime();
	        if (conf.isForceFromStart()) {
	            startOffsetTime = conf.getStartOffsetTime();
	        }
	        return getOffset(consumer, topic, partition, startOffsetTime);
	    }
	 
	 public static long getOffset(SimpleConsumer consumer, String topic, int partition, long startOffsetTime) {
	     
		 ///////////////////////
		 
		 
		 //////////////////////////
		 
		 
		 TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
	        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
	        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(startOffsetTime, 1));
	        OffsetRequest request = new OffsetRequest(
	                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
	        long[] offsets = consumer.getOffsetsBefore(request).offsets(topic, partition);
	        if (offsets.length > 0) {
	            return offsets[0];
	        } else {
	            return NO_OFFSET;
	        }
	    }
	 
	 

	public static ByteBufferMessageSet fetchMessages(Configuration conf, SimpleConsumer consumer,int partitionId, long offset) throws UpdateOffsetException {
		ByteBufferMessageSet msgs = null;
		FetchRequestBuilder builder = new FetchRequestBuilder();
		FetchRequest fetchRequest = builder.addFetch(conf.getTopics(), partitionId, offset, conf.getFetchSizeBytes()).
                clientId(conf.getClientId()).maxWait(conf.getFetchMaxWait()).build();
		FetchResponse fetchResponse;
		
		try {
            fetchResponse = consumer.fetch(fetchRequest);
        } catch (Exception e) {
            if (e instanceof ConnectException ||
                    e instanceof SocketTimeoutException ||
                    e instanceof IOException ||
                    e instanceof UnresolvedAddressException
                    ) {
                System.out.println("Network error when fetching messages:"+ e.getMessage());
                throw new FailedFetchException(e);
            } else {
                throw new RuntimeException(e);
            }
        }
        if (fetchResponse.hasError()) { // 主要处理offset outofrange的case，通过getOffset从earliest或latest读
            KafkaError error = KafkaError.getError(fetchResponse.errorCode(conf.getTopics(), partitionId));
            if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE) && conf.isUseStartOffsetTimeIfOffsetOutOfRange()) {
            	System.out.println("Got fetch request with offset out of range: [" + offset + "]; " +
                        "retrying with default start offset time from configuration. " +
                        "configured start offset time: [" + conf.getSocketTimeoutMs() + "]");
                throw new UpdateOffsetException();//pdateOffsetException
            } else {
                String message = "Error fetching data from [" + partitionId + "] for topic [" + conf.getTopics() + "]: [" + error + "]";
                System.out.println(message);
                throw new FailedFetchException(message);
            }
        } else {
            msgs = fetchResponse.messageSet(conf.getTopics(), partitionId);
           
        }
        return msgs;
    }
	
	
	public static String getClientId(String client, String partitionid){
		return "Client_"+client+"_"+partitionid;
	}
	

	/**
	 * /sync/topics/ids/[topic_name]
	 * @return
	 */
	public static String getSyncTopicsPath(){
		return "/sync/topics/ids";
	}
	
	
	public static void main(String[] args) {
		
	}

}
