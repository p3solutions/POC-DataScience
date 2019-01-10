package com.learn.logging.optimised;

import java.io.File;
import java.io.FileNotFoundException;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.learn.logging.logger.JobLogger;


public class StartAnalysis {

	public static void main(String[] args) throws InterruptedException, SQLException, ClassNotFoundException, FileNotFoundException {
		OMThreadedIM obj = new OMThreadedIM();
		
		JobLogger.getLogger().info(StartAnalysis.class.getName(), "main method", "Start");

		CPInstance connObj = new CPInstance();
		Connection conn = null;

		int size;
		Trie trie;
		
		System.out.println("Started");
		
		Instant start = Instant.now();
		
		ResultSet rs;
		String col1;
            ExecutorService executor = Executors.newFixedThreadPool(5);

		try {
			conn =connObj.getConnectionFRomPool();
			if (conn != null)
				JobLogger.getLogger().info(StartAnalysis.class.getName(), "main method", "Connection Established");

//			new GenerateMetadata1(conn).generate();
//	        JSONParser parser = new JSONParser();
//            Object parse = parser.parse(new FileReader("md.json"));
            
			new GenerateMetadata(conn).generate();
			Thread.currentThread().join();
			
//			String path = "/Users/admin/Projects/Data-Science-POC/V1.1/metadata_PS/";
			String path = "C:/metadata_sakila/";
            File file = new File(path);
            File[] files = file.listFiles();
            JSONParser parser = new JSONParser();

            for(File f: files) {
            	System.out.println("File name "+f);
            Object parse = parser.parse(new FileReader(path+f.getName()));

            JSONObject jsonObject = (JSONObject) parse;
            JSONObject tab = (JSONObject) jsonObject.get("table");
            List<String> tableList = new ArrayList<>();
            for(Object k : tab.keySet()) {
            	tableList.add((String)k);
            }
            int nTab = tableList.size();
            
            System.out.println(" No. of tables in this thread = "+nTab);
			for (int i = 0; i < tableList.size(); i++) {
                String table1 = tableList.get(i);
                JSONObject details1 = (JSONObject) tab.get(table1);
                Set<String> colSet1 = ((HashMap) details1.get("columns")).keySet();
        		List<String> colNames1;
                colNames1 = new ArrayList<>();
                for(String st:colSet1) {
                	colNames1.add(st);
                }

				for (int j = 0; j < colNames1.size(); j++) {
					col1 = colNames1.get(j);
//					String colDataFetchQry = "select `" + col1 + "` from " + tableList.get(i);
					String colDataFetchQry = "select " + col1 + " from " + tableList.get(i);
	                String type1 = (String)((HashMap) details1.get("columns")).get(col1);

					rs = obj.getRecords(conn, colDataFetchQry);
					GetResults complex = obj.constructTrie(rs, col1);
					trie = complex.trie;
					size = complex.size;

					rs.close();

					for (int k = i + 1; k < tableList.size(); k++) {
						String table2 = tableList.get(k);
						
		                JSONObject details2 = (JSONObject) tab.get(table2);
		                Set<String> colSet2 = ((HashMap) details2.get("columns")).keySet();
		        		List<String> colNames2;

		                colNames2 = new ArrayList<>();
		                for(String st:colSet2) {
		                	colNames2.add(st);
		                }
						for(String col2 : colNames2) {

			                String type2 = (String)((HashMap) details2.get("columns")).get(col2);
			                if(type1.equals(type2)) {
//								t1 = new OMThreadedThread(col2, tableList.get(k), trie, size, tableList.get(i), col1, obj1 );
//								t1.start();
//								threads.add(t1);
			                    Thread worker = new WorkerThread(col2, tableList.get(k), trie, size, tableList.get(i), col1, connObj);
			                    executor.execute(worker);

//								threads.add((Thread) worker);
			                }
						}
					}
				}
			}
            }
		} catch (Exception e) {
			JobLogger.getLogger().info(StartAnalysis.class.getName(), "main method", e.getMessage());
			System.out.println(e.getMessage());

		}

//		for(Thread t : threads) {
//			t.join();
//		}
		if(executor.isTerminated()) {
			Duration timeElapsed = Duration.between(start, Instant.now());
			JobLogger.getLogger().info(StartAnalysis.class.getName(), "main method", "Time elapsed " + timeElapsed);
			System.out.println("timeElapsed "+timeElapsed);
			System.out.println("Done");
		}
		executor.shutdown();

	}
}