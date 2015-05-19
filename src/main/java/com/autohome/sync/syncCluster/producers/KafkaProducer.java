package com.autohome.sync.syncCluster.producers;

import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

	public ProducerConfig config =null;
	public Producer producer=null;
	
	public void init(){
		Properties props = new Properties();
		
		props.put("metadata.broker.list", ProducerConf.METADATA_BROKER_LIST);
		props.put("serializer.class", "com.autohome.kafka.LogMessageT");
		props.put("partitioner.class", "com.autohome.kafka.SimplePartitioner");
	    props.put("request.required.acks", "1");
	    props.put("message.send.max.retries", "100");
	    props.put("producer.type", "async");
	    props.put("batch.num.messages", "100");
	    config = new ProducerConfig(props);
	    producer = new Producer(config);
	}
	
	
	public void close(final Producer producer){
		if(producer!=null){
			producer.close();
		}
	}
	
	public void send(List<kafka.producer.KeyedMessage<String, String>> o) {
		producer.send(o);
	}
}
