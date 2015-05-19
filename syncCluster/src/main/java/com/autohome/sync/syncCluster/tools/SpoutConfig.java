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
import java.util.List;

/**
 * 
 * @author nxcjh
 *
 */
public class SpoutConfig extends KafkaConfig implements Serializable {
    public List<String> zkServers = null; //zk hosts列表, 格式就是简单ip:xxxx.xxxx.xxxx.xxxx
    public Integer zkPort = null;	//zk端口号, 一般是2181
    public String zkRoot = null;	//该参数是consumer消费的meta信息, 保存在zk的路径, 自己指定
    public String id = null;	//唯一id
    public long stateUpdateIntervalMs = 2000; //commit消费的offset到zk的时间间隔.

    public SpoutConfig(BrokerHosts hosts, String topic, String zkRoot, String id) {
        super(hosts, topic);
        this.zkRoot = zkRoot;
        this.id = id;
    }
}
