package com.learn.logging.optimised;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class GenerateMetadata {
	Connection conn;
	public GenerateMetadata(Connection conn) {
		this.conn = conn;
	}
	public void generate() {
		ArrayList<Thread> threads = new ArrayList<>();

		try {
			ResultSet rst = conn.getMetaData().getTables(null, null, "%", null);
			int tnum=0;
			while (rst.next()) {
				tnum++;
			}
			System.out.println("Total No of tables = "+tnum);
			int nList = 9; int listSize = 10000; 
//			int nList = 3; int listSize = 10; 

			rst = conn.getMetaData().getTables(null, null, "%", null);
			
			List<List<String>> lists = new ArrayList<List<String>>();
			RunClass th[] = new RunClass[nList];

			for(int i = 0; i <nList; i++) {
				List<String> innerList = new ArrayList<String>();

				for(int j = 0; j< listSize; j++) {
					if(rst.next()){
						String t = rst.getString(3);
						innerList.add(t);
					}
				}
				lists.add(i, innerList);

			}
	        CountDownLatch latch = new CountDownLatch(nList); 

			for(int i = 0 ;i<nList;i++) {
				th[i] = new RunClass(conn, lists.get(i), i, latch);
				threads.add(th[i]);
				th[i].start();
			}
			latch.await();
			System.out.println("Generate has finished"); 
			
//			for(Thread t : threads) {
//				t.join();
//			}
		}catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
	}
}
