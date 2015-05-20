package com.autohome.sync.syncCluster.producers;

public class ProducerConf {

	public static final String METADATA_BROKER_LIST = "10.168.100.182:9092";
	public static final String SERIALIZER_CLASS = "com.autohome.kafka.BufferEncoder";
	public static final String PARTITIONER_CLASS = "com.autohome.kafka.SimplePartitioner";
	public static final String REQUEST_REQUIRED_ACKS = "1";
	public static final String MESSAGE_SEND_MAX_RETIES = "100";
	public static final String BATCH_NUM_KEY = "batch.num.messages";
	public static final int BATCH_NUM_VALUE=100;
}
