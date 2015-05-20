package com.autohome.sync.syncCluster.producers;


import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.ProducerConfig;

public class SyncProducer {

	private static final Logger LOG = Logger.getLogger(SyncProducer.class);
	public Producer<String, String> producer;
	
	public void init(){
		 Properties props = new Properties();   
         props.setProperty("metadata.broker.list",ProducerConf.METADATA_BROKER_LIST);   
         props.setProperty("serializer.class",ProducerConf.SERIALIZER_CLASS);   
         props.put("request.required.acks","1");   
         props.put("producer.type", "async");
         props.put("partitioner.class", ProducerConf.PARTITIONER_CLASS);
         props.put("batch.num.messages", ProducerConf.MESSAGE_SEND_MAX_RETIES);
         ProducerConfig config = new ProducerConfig(props);   
         producer = new Producer<String, String>(config); 
	}
	
	
	public void send(List<kafka.producer.KeyedMessage<String, String>> o){
		producer.send(o);
	}
	
	public void close(){
		if(producer!=null){
			producer.close();
		}
	}
	
	
	static List<kafka.producer.KeyedMessage<String, Message>> list;
	public static void main(String[] args) {
//		SyncProducer p = new SyncProducer();
//		p.init();
//		 list = new ArrayList<kafka.producer.KeyedMessage<String, Message>>();
//		 try {   
//             int i =1; 
//             while(i < 1000){ 
//            	 Message msg = new Message(UUID.randomUUID().toString().getBytes());
//            	 KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>("kafka-replica",i+"",msg);   
//                 list.add(data);
////            	 System.out.println(Utils.toByteArray(data.message().payload()));
//                 i++;
//             } 
//             p.send(list);
//             System.out.println(list.size());
//             list.clear();
//         } catch (Exception e) {   
//             e.printStackTrace();   
//         }finally{
//        	 p.close();   
//         }
//       
	}
}
