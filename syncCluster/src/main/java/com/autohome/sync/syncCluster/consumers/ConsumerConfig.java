package com.autohome.sync.syncCluster.consumers;

public class ConsumerConfig {

	public static String zkHosts = "10.168.100.182:2181";
	public static int zkPort = 9092;
	public static String topics = "sync_0";
	public static long maxReads = 200;
	public static int socketTimeoutMs = 200000;
	public static int bufferSizeBytes = 1024 * 1024;
	public static String clientId = "sync";
	public static boolean forceFromStart = false;
	public static long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
	public static Long maxOffsetBehind = Long.MAX_VALUE;
	public static int fetchSizeBytes =1024 * 1024;
	public static int fetchMaxWait = 100000;
	public static boolean useStartOffsetTimeIfOffsetOutOfRange = true;
	public static String topologyInstanceId = "";
	public static long refreshFreqSecs = 60;
	
	
}
