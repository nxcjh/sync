package com.autohome.sync.syncCluster.tools;

public class ZKHosts {

	private static final String DEFAULT_ZK_PATH = "/brokers";

    public String brokerZkStr = null;
    public String brokerZkPath = null; // e.g., /kafka/brokers
    public int refreshFreqSecs = 60;

    public ZKHosts(String brokerZkStr, String brokerZkPath) {
        this.brokerZkStr = brokerZkStr;
        this.brokerZkPath = brokerZkPath;
    }
}
