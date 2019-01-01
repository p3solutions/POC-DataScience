package com.learn.logging.optimised;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.learn.logging.logger.JobLogger;

public class OMThreaded {

//	private static final int MAX_THREAD = 5;
//	public static int threadcounter = 0;

	public static void main(String[] args) throws InterruptedException, SQLException {
		OMThreadedIM obj = new OMThreadedIM();
		
		JobLogger.getLogger().info(OMThreaded.class.getName(), "main method", "Start");

		CPDemo obj1 = new CPDemo();
		Connection conn = null;

		final List<String> tableList = new ArrayList<>();
		int size;
		Trie trie;
		
		System.out.println("Started");
		
		Instant start = Instant.now();
		
		OMThreadedThread t1;
		List<String> colNames1;
		List<String> colNames2;
		ResultSet rs;

		
		List<Thread> threads = new LinkedList<>();
//		String[] c1;
		try {

			conn =obj1.getConnectionFRomPool();
			if (conn != null)
				JobLogger.getLogger().info(OMThreaded.class.getName(), "main method", "Connection Established");
			ResultSet rst = conn.getMetaData().getTables(null, null, "%", null);
			String col1;
//			String[] al = new String[] {"PS_PENDING_ITEM", //1706
//					"PS_SET_CNTRL_REC_S"//123702
//					};
/*
			while (rst.next()) {
				tableList.add(rst.getString(3));
			}

			JobLogger.getLogger().info(OMThreaded.class.getName(), "main method", "Table List obtained");
//			for(String d: al)
//				tableList.add(d);
//				
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
//					String colDataFetchQry = "select " + col1 + " from " + tableList.get(i);

					rs = obj.getRecords(conn, colDataFetchQry);
					GetResults complex = obj.constructTrie(rs, col1);
					trie = complex.trie;
					size = complex.size;
//					
//					GetResults1 complex = obj.getCol1(rs, col1);
//					c1 = complex.col1;
//					size = complex.size;

					rs.close();

					for (int k = i + 1; k < tableList.size(); k++) {
						String secondaryTableQry = "SELECT * FROM " + tableList.get(k);
						rs = obj.getRecords(conn, secondaryTableQry);
						ArrayList<String> secondaryColNames = obj.getColumnNames(rs);
						rs.close();
						for(String s : secondaryColNames) {
							t1 = new OMThreadedThread(s, tableList.get(k), trie, size, tableList.get(i), col1, obj1 );
//							t1 = new OMThreadedThread(s, tableList.get(k), size, tableList.get(i), col1, obj1, c1 );

							
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
		*/
	        JSONParser parser = new JSONParser();
	        
	   	 
            Object parse = parser.parse(new FileReader("md.json"));
 
            JSONObject jsonObject = (JSONObject) parse;
            JSONObject tab = (JSONObject) jsonObject.get("table");
            for(Object k : tab.keySet()) {
            	tableList.add((String)k);
            }
//            System.out.println("TableList "+tableList);
			for (int i = 0; i < tableList.size(); i++) {
                String table1 = tableList.get(i);
//                System.out.println("table1 "+table1);
                JSONObject details1 = (JSONObject) tab.get(table1);
                Set<String> colSet1 = ((HashMap) details1.get("columns")).keySet();
                colNames1 = new ArrayList<>();
                for(String st:colSet1) {
                	colNames1.add(st);
                }

				for (int j = 0; j < colNames1.size(); j++) {
					col1 = colNames1.get(j);
//					System.out.println("column 1"+col1);
					String colDataFetchQry = "select `" + col1 + "` from " + tableList.get(i);
//					String colDataFetchQry = "select " + col1 + " from " + tableList.get(i);

					rs = obj.getRecords(conn, colDataFetchQry);
					GetResults complex = obj.constructTrie(rs, col1);
					trie = complex.trie;
					size = complex.size;

					rs.close();

					for (int k = i + 1; k < tableList.size(); k++) {
						String table2 = tableList.get(k);
//						System.out.println("table2 "+table2);
						String secondaryTableQry = "SELECT * FROM " + table2;
						rs = obj.getRecords(conn, secondaryTableQry);
						
		                JSONObject details2 = (JSONObject) tab.get(table2);
		                Set<String> colSet2 = ((HashMap) details2.get("columns")).keySet();
		                colNames2 = new ArrayList<>();
		                for(String st:colSet2) {
		                	colNames2.add(st);
		                }

						rs.close();
						for(String col2 : colNames2) {
//							System.out.println("Column2 "+col2);
							t1 = new OMThreadedThread(col2, tableList.get(k), trie, size, tableList.get(i), col1, obj1 );

							
							t1.start();
							threads.add(t1);
						}
					}
					t1 = null;
				}
			}
		} catch (Exception e) {
			JobLogger.getLogger().info(OMThreaded.class.getName(), "main method", e.getMessage());
			System.out.println(e.getMessage());
		}

		for(Thread t : threads) {
			t.join();
		}
		Duration timeElapsed = Duration.between(start, Instant.now());
		JobLogger.getLogger().info(OMThreaded.class.getName(), "main method", "Time elapsed " + timeElapsed);
		System.out.println("timeElapsed "+timeElapsed);
		System.out.println("Done");
	}
}