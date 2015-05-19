package com.autohome.sync.syncCluster.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CallableDemo {

	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		ExecutorService executorService = Executors.newCachedThreadPool();
		List<Future<String>> resultList = new ArrayList<Future<String>>();	
		//创建10个任务并执行
		for(int i=0; i<2; i++){
			//使用ExecutorService执行Callable类型的任务, 并将结果保存到future变量中.
			Future<String> future = executorService.submit(new TaskWithResult(i));
			resultList.add(future);
		}
		
		//遍历任务的结果
		for(Future<String> fs : resultList){
			System.out.println(fs.get());
		}
		//启动一次顺序关闭, 执行以前提交的任务, 但不接受新任务, 如果已经关闭, 则调用没有其他作用.
		executorService.shutdown();
	}
}
