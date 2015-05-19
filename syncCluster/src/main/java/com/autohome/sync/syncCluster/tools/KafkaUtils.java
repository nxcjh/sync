package com.autohome.sync.syncCluster.tools;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.util.HashMap;
import java.util.Map;










import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;


public class KafkaUtils {

	private static final int NO_OFFSET = -5;
	
	public static long getOffset(SimpleConsumer consumer,String topic, int partition, KafkaConfig config){
		long startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		if(config.forceFromStart){
			startOffsetTime = config.startOffsetTime;
		}
		return getOffset(consumer,topic,partition,startOffsetTime);
	}

	public static long getOffset(SimpleConsumer consumer, String topic,
			int partition, long startOffsetTime) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,partition);
		Map<TopicAndPartition,PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition,PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(startOffsetTime,1));
		OffsetRequest request = new OffsetRequest(requestInfo,kafka.api.OffsetRequest.CurrentVersion(),consumer.clientId());
		long[] offsets = consumer.getOffsetsBefore(request).offsets(topic, partition);
		 if (offsets.length > 0) {
	            return offsets[0];
	        } else {
	            return NO_OFFSET;
	        }

	}

	//consumer fetchMessage过程
	public static ByteBufferMessageSet fetchMessages(KafkaConfig config,
			SimpleConsumer _consumer, Partition _partition, long offset) throws UpdateOffsetException {
		ByteBufferMessageSet msgs = null;
		String topic = config.topic;
		int partitionId = _partition.partition;
		FetchRequestBuilder builder = new FetchRequestBuilder();
		FetchRequest fetchRequest = builder.addFetch(topic, partitionId,offset, config.fetchSizeBytes).
				clientId(config.clientId).maxWait(config.fetchMaxWait).build();
		FetchResponse fetchResponse;
		try{
			fetchResponse = _consumer.fetch(fetchRequest);
		} catch (Exception e) {
            if (e instanceof ConnectException ||
                    e instanceof SocketTimeoutException ||
                    e instanceof IOException ||
                    e instanceof UnresolvedAddressException
                    ) {
                System.out.println("Network error when fetching messages:"+e.getMessage());
                throw new FailedFetchException(e);
            } else {
                throw new RuntimeException(e);
            }
        }
		 if (fetchResponse.hasError()) { // 主要处理offset outofrange的case，通过getOffset从earliest或latest读
	            KafkaError error = KafkaError.getError(fetchResponse.errorCode(topic, partitionId));
	            if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE) && config.useStartOffsetTimeIfOffsetOutOfRange) {
	               System.out.println("Got fetch request with offset out of range: [" + offset + "]; " +
	                        "retrying with default start offset time from configuration. " +
	                        "configured start offset time: [" + config.startOffsetTime + "]");
	                throw new UpdateOffsetException();//pdateOffsetException
	            } else {
	                String message = "Error fetching data from [" + _partition + "] for topic [" + topic + "]: [" + error + "]";
	                System.out.println(message);
	                throw new FailedFetchException(message);
	            }
	        } else {
	            msgs = fetchResponse.messageSet(topic, partitionId);
	           
	        }
	        return msgs;
	}

	public static IBrokerReader makeBrokerReader(Map conf, KafkaConfig	 config) {

		
		return new ZkBrokerReader(conf, config.topic, (ZKHosts) config.hosts);
	}
}
