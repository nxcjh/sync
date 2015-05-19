package com.autohome.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import kafka.utils.VerifiableProperties;

import com.google.protobuf.InvalidProtocolBufferException;

public class LogMessageT extends kafka.serializer.StringEncoder{

	byte[] byteArray = null;
	KafkaEncoder.Logger.Builder loggerBuilder = KafkaEncoder.Logger.newBuilder();
	ByteArrayOutputStream out = new ByteArrayOutputStream();
	KafkaEncoder.Logger logger= null;
	
	public LogMessageT(VerifiableProperties props) {
		super(props);
		 String encoding = "UTF8";	    
         props.getString("serializer.encoding", "UTF8");
	}

	
	public byte[] toBytes(String log) {
		out.reset();
		loggerBuilder.setLog(log);
		logger= loggerBuilder.build();
		byteArray = logger.toByteArray();
		return byteArray;
	}
	
	
	public void decoder(byte[] byteArray) throws InvalidProtocolBufferException{
		ByteArrayInputStream input = new ByteArrayInputStream(byteArray);
		// 反序列化
	    KafkaEncoder.Logger logger = KafkaEncoder.Logger.parseFrom(byteArray);
	    System.out.println(logger.getLog());
	}
	
//	public static void main(String[] args) throws InvalidProtocolBufferException {
//		LogMessageT log = new LogMessageT();
//		byte[] bytea = log.toBytes("崔金辉");
//		log.decoder(bytea);
//		
//	}
}
