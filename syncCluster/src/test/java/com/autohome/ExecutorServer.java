package com.autohome;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorServer {
	public 	 static String data = "------------";
	static ExecutorService server =null;
	private static boolean isTrue = true;
	static class Task implements Runnable{

		
		public void run() {
			while(ExecutorServer.isTrue){
				System.out.println(data);
			}
		}
		static class Task2 implements Runnable{

			public void run() {
				while(ExecutorServer.isTrue){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.out.println("##"+data);
				}
			}
		}
	
	public static void main(String[] args) {
		server = Executors.newFixedThreadPool(1);
		server.execute(new Task());
		
		try {
			Thread.sleep(10000);
			server.shutdownNow();
			
			server = Executors.newFixedThreadPool(2);
			data = "000000000000000";
			server.execute(new Task2());
			//server.execute(new Task());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
	}

	
