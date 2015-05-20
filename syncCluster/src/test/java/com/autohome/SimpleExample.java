package com.autohome;
 
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
 







import kafka.producer.KeyedMessage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import backtype.storm.utils.Utils;

import com.autohome.sync.syncCluster.producers.SyncProducer;
 
public class SimpleExample {
	static List<kafka.producer.KeyedMessage<String, Message>> list;
	static SyncProducer producer;
  public static void main(String args[]) {
	  producer = new SyncProducer();
	  producer.init();
	  list = new ArrayList<kafka.producer.KeyedMessage<String, Message>>();
    SimpleExample example = new SimpleExample();
    long maxReads = Long.parseLong(100+"");
    String topic = "kafka-replica";
    int partition = Integer.parseInt("0");
    List<String> seeds = new ArrayList<String>();
    seeds.add("10.168.100.182");
    int port = Integer.parseInt("9092");
    try {
      example.run(maxReads, topic, partition, seeds, port);
    } catch (Exception e) {
      System.out.println("Oops:" + e);
       e.printStackTrace();
    }
  }
 
  private List<String> m_replicaBrokers = new ArrayList<String>();
 
  public SimpleExample() {
    m_replicaBrokers = new ArrayList<String>();
  }
 
  public void run(long a_maxReads, String a_topic, int a_partition, List<String> a_seedBrokers, int a_port) throws Exception {
    // find the meta data about the topic and partition we are interested in
    //
    PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition);
    if (metadata == null) {
      System.out.println("Can't find metadata for Topic and Partition. Exiting");
      return;
    }
    if (metadata.leader() == null) {
      System.out.println("Can't find Leader for Topic and Partition. Exiting");
      return;
    }
    String leadBroker = metadata.leader().host();
    String clientName = "Client_" + a_topic + "_" + a_partition;
 
    SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
    long readOffset = getOffset(consumer,a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime());
 
    int numErrors = 0;
    while (a_maxReads > 0) {
      if (consumer == null) {
        consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
      }
      FetchRequest req = new FetchRequestBuilder()
          .clientId(clientName)
          .addFetch(a_topic, a_partition, readOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
          .build();
      FetchResponse fetchResponse = consumer.fetch(req);
 
      if (fetchResponse.hasError()) {
        numErrors++;
        // Something went wrong!
        short code = fetchResponse.errorCode(a_topic, a_partition);
        System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
        if (numErrors > 5) break;
        if (code == ErrorMapping.OffsetOutOfRangeCode())  {
          // We asked for an invalid offset. For simple case ask for the last element to reset
          readOffset = getOffset(consumer,a_topic, a_partition, kafka.api.OffsetRequest.LatestTime());
          continue;
        }
        consumer.close();
        consumer = null;
        leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
        continue;
      }
      numErrors = 0;
 
      long numRead = 0;
      for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {

		long currentOffset = messageAndOffset.offset();
        if (currentOffset < readOffset) {
          System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
          continue;
        }
        readOffset = messageAndOffset.nextOffset();
        ByteBuffer payload = messageAndOffset.message().payload();
 
        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);
        KeyedMessage<String, Message> data = new KeyedMessage<String, Message>("kafka-replica",messageAndOffset.message());   
        list.add(data);
        producer.send(list);
        list.clear();
        
        System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
        numRead++;
        a_maxReads--;
      }
 
      if (numRead == 0) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }
    }
    if (consumer != null) consumer.close();
  }
 

  public static long getOffset(SimpleConsumer consumer, String topic, int partition, long startOffsetTime) {
      TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
      Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
      requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(startOffsetTime, 1));
      OffsetRequest request = new OffsetRequest(
              requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
      long[] offsets = consumer.getOffsetsBefore(request).offsets(topic, partition);
      if (offsets.length > 0) {
          return offsets[0];
      } else {
          return 0l;
      }
  }
  
  
  
  
  private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
    for (int i = 0; i < 3; i++) {
      boolean goToSleep = false;
      PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
      if (metadata == null) {
        goToSleep = true;
      } else if (metadata.leader() == null) {
        goToSleep = true;
      } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
        // first time through if the leader hasn't changed give ZooKeeper a second to recover
        // second time, assume the broker did recover before failover, or it was a non-Broker issue
        //
        goToSleep = true;
      } else {
        return metadata.leader().host();
      }
      if (goToSleep) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }
    }
    System.out.println("Unable to find new leader after Broker failure. Exiting");
    throw new Exception("Unable to find new leader after Broker failure. Exiting");
  }
 
  private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
    PartitionMetadata returnMetaData = null;
    loop:
    for (String seed : a_seedBrokers) {
      SimpleConsumer consumer = null;
      try {
        consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");
        List<String> topics = Collections.singletonList(a_topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
 
        List<TopicMetadata> metaData = resp.topicsMetadata();
        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            if (part.partitionId() == a_partition) {
              returnMetaData = part;
              break loop;
            }
          }
        }
      } catch (Exception e) {
        System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
            + ", " + a_partition + "] Reason: " + e);
      } finally {
        if (consumer != null) consumer.close();
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
}