/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.autohome.sync.syncCluster.tools;



import java.io.Serializable;

/**
 * 
 * @author nxcjh
 *
 */
public class KafkaConfig implements Serializable {

	//一个接口, 实现类有ZKHosts, StatisHosts
    public final BrokerHosts hosts;
    public final String topic;
    public final String clientId;

    public int fetchSizeBytes = 1024 * 1024;//每次从Kafka读取byte数据, 这个变量会在KafkaUtils的fetchMessage方法中看到,
    public int socketTimeoutMs = 200000;//consumer 连接 kafka server超时时间
    public int fetchMaxWait = 100000;
    public int bufferSizeBytes = 1024 * 1024; //consumer端缓存大小
//    public MultiScheme scheme = new RawMultiScheme(); //数据发送的序列化和反序列化定义的Scheme,
    public boolean forceFromStart = false; //和startOffsetTime, 一起用, 默认情况下, 为false, 一旦startOffsetTime被设置, 就要设置为true
    public long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();//-2, 从kafka头开始, -1, 从最新的开始, 0 = 无, 从zk开始
    public long maxOffsetBehind = Long.MAX_VALUE; //每次Kafka会读取一批offset存放在list中, 当zk offset比当前本地保存的commitOffset相减大于这个值时, 重新设置commitOffset为当前zkOffset, 代码详见PartitionManager
    public boolean useStartOffsetTimeIfOffsetOutOfRange = true;
    public int metricsTimeBucketSizeInSecs = 60;

    public KafkaConfig(BrokerHosts hosts, String topic) {
        this(hosts, topic, kafka.api.OffsetRequest.DefaultClientId());
    }

    public KafkaConfig(BrokerHosts hosts, String topic, String clientId) {
        this.hosts = hosts;
        this.topic = topic;
        this.clientId = clientId;
    }

}
