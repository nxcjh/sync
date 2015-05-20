package com.autohome.sync.syncCluster.tools;

public class Configuration {
	
	public  String zkHosts;
	public  int zkPort = 9092;
	public  String topics = "sync_0";
	public  long maxReads = 200;
	public  int socketTimeoutMs = 200000;
	public  int bufferSizeBytes = 1024 * 1024;
	public  String clientId = "sync";
	public  boolean forceFromStart = false;
	public  long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
	public  Long maxOffsetBehind = Long.MAX_VALUE;
	public  int fetchSizeBytes =1024 * 1024;
	public  int fetchMaxWait = 100000;
	public  boolean useStartOffsetTimeIfOffsetOutOfRange = true;
	public  String topologyInstanceId = "";
	public  long refreshFreqSecs = 60;
	public String getZkHosts() {
		return zkHosts;
	}
	public void setZkHosts(String zkHosts) {
		this.zkHosts = zkHosts;
	}
	public int getZkPort() {
		return zkPort;
	}
	public void setZkPort(int zkPort) {
		this.zkPort = zkPort;
	}
	public String getTopics() {
		return topics;
	}
	public void setTopics(String topics) {
		this.topics = topics;
	}
	public long getMaxReads() {
		return maxReads;
	}
	public void setMaxReads(long maxReads) {
		this.maxReads = maxReads;
	}
	public int getSocketTimeoutMs() {
		return socketTimeoutMs;
	}
	public void setSocketTimeoutMs(int socketTimeoutMs) {
		this.socketTimeoutMs = socketTimeoutMs;
	}
	public int getBufferSizeBytes() {
		return bufferSizeBytes;
	}
	public void setBufferSizeBytes(int bufferSizeBytes) {
		this.bufferSizeBytes = bufferSizeBytes;
	}
	public String getClientId() {
		return clientId;
	}
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	public boolean isForceFromStart() {
		return forceFromStart;
	}
	public void setForceFromStart(boolean forceFromStart) {
		this.forceFromStart = forceFromStart;
	}
	public long getStartOffsetTime() {
		return startOffsetTime;
	}
	public void setStartOffsetTime(long startOffsetTime) {
		this.startOffsetTime = startOffsetTime;
	}
	public Long getMaxOffsetBehind() {
		return maxOffsetBehind;
	}
	public void setMaxOffsetBehind(Long maxOffsetBehind) {
		this.maxOffsetBehind = maxOffsetBehind;
	}
	public int getFetchSizeBytes() {
		return fetchSizeBytes;
	}
	public void setFetchSizeBytes(int fetchSizeBytes) {
		this.fetchSizeBytes = fetchSizeBytes;
	}
	public int getFetchMaxWait() {
		return fetchMaxWait;
	}
	public void setFetchMaxWait(int fetchMaxWait) {
		this.fetchMaxWait = fetchMaxWait;
	}
	public boolean isUseStartOffsetTimeIfOffsetOutOfRange() {
		return useStartOffsetTimeIfOffsetOutOfRange;
	}
	public void setUseStartOffsetTimeIfOffsetOutOfRange(
			boolean useStartOffsetTimeIfOffsetOutOfRange) {
		this.useStartOffsetTimeIfOffsetOutOfRange = useStartOffsetTimeIfOffsetOutOfRange;
	}
	public String getTopologyInstanceId() {
		return topologyInstanceId;
	}
	public void setTopologyInstanceId(String topologyInstanceId) {
		this.topologyInstanceId = topologyInstanceId;
	}
	public long getRefreshFreqSecs() {
		return refreshFreqSecs;
	}
	public void setRefreshFreqSecs(long refreshFreqSecs) {
		this.refreshFreqSecs = refreshFreqSecs;
	}
	
	
}