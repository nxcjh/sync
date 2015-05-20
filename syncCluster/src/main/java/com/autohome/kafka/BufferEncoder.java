package com.autohome.kafka;

import backtype.storm.utils.Utils;
import kafka.message.Message;
import kafka.utils.VerifiableProperties;

public class BufferEncoder implements kafka.serializer.Encoder<Message>{

	public BufferEncoder(VerifiableProperties props) {
		super();
		 String encoding = "UTF8";	    
        props.getString("serializer.encoding", "UTF8");
	}
	
	public byte[] toBytes(Message msg) {
		
		return Utils.toByteArray(msg.payload());
	}
}
