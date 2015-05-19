package com.autohome.sync.syncCluster.tools;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
 
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
 
public class KafkaConsumer {
    private static List<String> m_replicaBrokers = new ArrayList<String>();
 
    public static void main(String[] args) throws UnsupportedEncodingException {
        String topic = "com-realdeliver-test";
        int partition = 0;
 
        List<String> seeds = new ArrayList<String>();
        seeds.add("10.168.100.182:9093");

 
        //获取metadata信息
        PartitionMetadata metadata = findLeader(seeds, topic, partition);
        
        //获取leader
        Broker broker = metadata.leader();
        List<Broker> replicas = metadata.replicas();
        
        System.out.println("Leader Broker Info : " + broker);
        System.out.println("Replicas Broker Info : " + replicas.size());
        
        for (Broker replica : replicas) {
            System.out.println("Replica Broker Info : " + replica);
        }
 
        String clientName = "Client_" + broker.host() + "_" + broker.port();
 
        String host = broker.host();
        int port = broker.port();
        SimpleConsumer consumer = new SimpleConsumer(host, port, 3 * 1000, 64 * 1024, clientName);
 
        long offset = getLastOffset(consumer, topic, partition,kafka.api.OffsetRequest.EarliestTime(), clientName);//
 
        System.out.println("last offset : " + offset);
 
        kafka.api.FetchRequest fetchRequest = new FetchRequestBuilder()
                .clientId(clientName).addFetch(host, port, offset, 100000).build();
        FetchResponse fetchResponse = consumer.fetch(fetchRequest);
 
        if (fetchResponse.hasError()) {
            // process fetch failed
        }
 
        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
            long currentOffset = messageAndOffset.offset();
            if (currentOffset < offset) {
                System.out.println("Found an old offset: " + currentOffset + " Expecting: " + offset);
                continue;
            }
            offset = messageAndOffset.nextOffset();
            ByteBuffer payload = messageAndOffset.message().payload();
 
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(String.valueOf(messageAndOffset.offset()) + ": "
                    + new String(bytes, "UTF-8"));
        }
 
    }
 
    private static PartitionMetadata findLeader(List<String> a_seedBrokers, String a_topic, int a_partition) {
 
        PartitionMetadata returnMetaData = null;
        loop: for (String seed : a_seedBrokers) {
            String[] array = seed.split(":");
            String host = array[0];
            int port = Integer.valueOf(array[1]);
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(host, port, 3 * 1000, 8 * 1024,"test-comsumer");
 
                List<String> topics = Collections.singletonList(a_topic);
                System.out.println("Topics : " + Arrays.toString(topics.toArray()));
 
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                System.out.println(req.describe(true));
 
                TopicMetadataResponse resp = consumer.send(req);
 
                List<TopicMetadata> metaData = resp.topicsMetadata();
 
                for (TopicMetadata item : metaData) {
                    System.out.println("Topic & Metadata : " + item.topic());
                    // for (PartitionMetadata data : item.partitionsMetadata())
                    // {
                    // System.out.println(data.);
                    // }
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed
                        + "] to find Leader for [" + a_topic + ", "
                        + a_partition + "] Reason: " + e);
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
 
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
 
        return returnMetaData;
    }
 
    public static long getLastOffset(SimpleConsumer consumer, String topic,
            int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic,partition);
 
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        PartitionOffsetRequestInfo offsetRequestInfo = new PartitionOffsetRequestInfo(whichTime, 1);
        requestInfo.put(topicAndPartition, offsetRequestInfo);
 
        OffsetRequest request = new OffsetRequest(requestInfo,
                kafka.api.OffsetRequest.CurrentVersion(), clientName);
 
        OffsetResponse response = consumer.getOffsetsBefore(request);
 
        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: "
                            + response.errorCode(topic, partition));
            return 0;
        }
 
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }
}
