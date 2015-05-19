package com.autohome.sync.syncCluster.tools;

import java.util.concurrent.Callable;

public class TaskWithResult implements Callable<String> {

	private int id;
	
	
	
	public TaskWithResult(int id) {
		this.id = id;
	}




	public String call() throws Exception {
		System.out.println("call()方法被自动调用, 干活..."+Thread.currentThread().getName());
		for(int i=99999; i>0; i--){
			System.out.println(i+"--call()方法被自动调用, 干活..."+Thread.currentThread().getName());
		}
		return "end";
	}

}
