package com.autohome.sync.syncCluster.tools;

import kafka.message.Message;

public class MessageAndRealOffset {

	public Message msg;
	public long offset;
	public MessageAndRealOffset(Message msg, long offset) {
		super();
		this.msg = msg;
		this.offset = offset;
	}
	
	
}
