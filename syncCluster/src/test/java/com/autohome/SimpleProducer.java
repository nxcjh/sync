package com.autohome;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;



import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {
	
	
	
	public static void main(String[] args) {
		 Properties props = new Properties();   
         props.setProperty("metadata.broker.list","10.168.100.182:9092");   
         props.setProperty("serializer.class","com.autohome.kafka.BufferEncoder");   
         props.put("request.required.acks","1");   
         props.put("producer.type", "async");
         props.put("partitioner.class", "com.autohome.kafka.SimplePartitioner");
         ProducerConfig config = new ProducerConfig(props);   
         Producer<String, Message> producer = new Producer<String, Message>(config); 
         List<kafka.producer.KeyedMessage<String, Message>> list = new ArrayList<kafka.producer.KeyedMessage<String, Message>>();
        
         
         try {   
             int i =1; 
             while(i < 10){ 
            	 KeyedMessage<String, Message> data = new KeyedMessage<String, Message>("kafka-replica",new Message(UUID.randomUUID().toString().getBytes()));   
                 list.add(data);
            	 System.out.println(toByteArray(data.message().payload()));
                 i++;
             } 
             producer.send(list);
         } catch (Exception e) {   
             e.printStackTrace();   
         }   
         producer.close();   
	}
	
	public static byte[] toByteArray(ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        return ret;
    }

}
