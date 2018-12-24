package com.learn.logging.optimised;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.learn.logging.logger.JobLogger;

public class OMThreaded {

//	private static final int MAX_THREAD = 5;
//	public static int threadcounter = 0;

	public static void main(String[] args) throws InterruptedException, SQLException {
		OMThreadedIM obj = new OMThreadedIM();
		
		JobLogger.getLogger().info(OMThreaded.class.getName(), "main method", "Start");
//		String url = "jdbc:mysql://localhost:3306/sakila", user = "root", password = "secret";
//		
//		Connection conn = obj.setConnection(url, user, password);
		CPDemo obj1 = new CPDemo();
		Connection conn = null;

		final List<String> tableList = new ArrayList<>();
		int size;
		
		Trie trie;
		
		System.out.println("Started");
		
		Instant start = Instant.now();
		
		OMThreadedThread t1;
		
		List<Thread> threads = new LinkedList<>();

		try {

			conn =obj1.getConnectionFRomPool();
			if (conn != null)
				JobLogger.getLogger().info(OMThreaded.class.getName(), "main method", "Connection Established");
			ResultSet rst = conn.getMetaData().getTables(null, null, "%", null);
			String col1;
			while (rst.next()) {
				tableList.add(rst.getString(3));
//				System.out.println(rst.getString(3));
			}

			JobLogger.getLogger().info(OMThreaded.class.getName(), "main method", "Table List obtained");
	
			rst = null;

			for (int i = 0; i < tableList.size(); i++) {
				String stmt = "SELECT * FROM " + tableList.get(i);
				ResultSet rs = obj.getRecords(conn, stmt);
				ArrayList<String> colNames = obj.getColumnNames(rs);
				rs.close();
				for (int j = 0; j < colNames.size(); j++) {
					col1 = colNames.get(j);
//					System.out.println(col1);
					String colDataFetchQry = "select `" + col1 + "` from " + tableList.get(i);
					rs = obj.getRecords(conn, colDataFetchQry);
					GetResults complex = obj.constructTrie(rs, col1);
					trie = complex.trie;
					size = complex.size;
					rs.close();

					for (int k = i + 1; k < tableList.size(); k++) {
						String secondaryTableQry = "SELECT * FROM " + tableList.get(k);
						rs = obj.getRecords(conn, secondaryTableQry);
						ArrayList<String> secondaryColNames = obj.getColumnNames(rs);
						rs.close();
						for(String s : secondaryColNames) {
							
							t1 = new OMThreadedThread(s, tableList.get(k), trie, size, tableList.get(i), col1, obj1 );
//					        Worker work = new Worker(4);
//					        work.Execute(t1);

							t1.start();
							threads.add(t1);
						}
//						t1.setCurrentThreadName(t1.getName());
//						common.addThread(t1.getName());
						
//						System.out.println("Thread size " + common.threadList.size() + " thread name " + t1.getName());
						
//						while (common.threadList.size() >= MAX_THREAD) {
//							System.out.println("sleeping");
//							Thread.sleep(1);}
//						t1.start();
					}
					t1 = null;
				}
			}
		} catch (Exception e) {
			JobLogger.getLogger().info(OMThreaded.class.getName(), "main method", e.getMessage());
			System.out.println(e.getMessage());
		}
		Duration timeElapsed = Duration.between(start, Instant.now());
//		JobLogger.getLogger().info(OMThreaded.class.getName(), "main method", "Time elapsed " + timeElapsed);
		System.out.println("timeElapsed "+timeElapsed);
		for(Thread t : threads) {
			t.join();
		}
		System.out.println("Done");
	}
}