package com.learn.logging.optimised;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.learn.logging.logger.JobLogger;

public class TableWiseAnalysis {
	Connection setConnection(String url, String user ,String password) {
		//check for mysql and mssql and return connection objects accordingly
		int i =1;
		try {
			if(i == 1) {
				Class.forName("com.mysql.jdbc.Driver");
				return DriverManager.getConnection(url, user, password);				
			}
			else {
	        	Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
	            return DriverManager.getConnection("jdbc:sqlserver://34.213.4.182:57997;databaseName=PS_FINANCE", "sa", "secret@P3");

			}
		}catch (Exception e) {
			JobLogger.getLogger().info(Optimised.class.getName(),"setConnection",e.getMessage());
			return null;
		}
	}

	public GetResults constructTrie(ResultSet rs1, String col1) throws SQLException {
		JobLogger.getLogger().info(Optimised.class.getName(), "constructTrie", "Before creating trie");
		int size = 0;
		Trie trie = new Trie();
		while (rs1.next()) {
			size++;
			if (rs1.getString(col1) == null)
				continue;
			trie.insert(rs1.getString(col1));
		}
		rs1 = null;
		GetResults o = new GetResults();
		o.trie =trie;
		o.size = size;
		JobLogger.getLogger().info(Optimised.class.getName(), "constructTrie", "After creating trie");

		return(o);		
	}
	public int findMatches(Trie trie, ResultSet rs, int size, String col2) throws SQLException{
		HashMap<String, Boolean> results = new HashMap<>();
		int count = 0;
		JobLogger.getLogger().info(Optimised.class.getName(), "findMatches method", "Comparison for secondary column started");
		
		while (rs.next()) {
			String param2 = rs.getString(col2);
			if (param2 != null)
				results.put(param2, trie.search(param2));

		}


		for (Map.Entry entry : results.entrySet()) {
			if ((Boolean) entry.getValue() == true) {
				count++;
			}
		}
		if(size!=0)
			return (count * 100 / size);
		else
			return -999;
	}
    public void printObjectSize(Object o) {
    	System.out.println("Object type: " + o.getClass() +
    	          ", size: " + InstrumentationAgent.getObjectSize(o) + " bytes");   
    	}
	public void garbageCollect() {
		System.out.println(" >> GC triggered");
		System.out.println(" Before GC :\t MAX : " + (Runtime.getRuntime().maxMemory() / 1048576) + " MB \t TOTAL : "
				+ (Runtime.getRuntime().totalMemory() / 1048576) + " MB \t FREE : "
				+ (Runtime.getRuntime().freeMemory() / 1048576) + " MB");
		System.gc();
		System.out.println(" After GC :\t MAX : " + (Runtime.getRuntime().maxMemory() / 1048576) + " MB \t TOTAL : "
				+ (Runtime.getRuntime().totalMemory() / 1048576) + " MB \t FREE : "
				+ (Runtime.getRuntime().freeMemory() / 1048576) + " MB");
		System.out.println("\n\n");
	}
	
	public static void main(String[] args) {
		OptimisedMod obj = new OptimisedMod();
		JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Start");
		String url = "jdbc:mysql://localhost:3306/employees",user = "root",password = "secret";
		Connection conn = obj.setConnection(url, user, password);
		final List<String> tableList = new ArrayList<>();
		int size, pct;
		Trie trie;
		System.out.println("Started");
		Instant start = Instant.now();
		TableToTableAnalysisRetrieveData input = new TableToTableAnalysisRetrieveData();
		HashMap<String, List<String>> sec = input.sec;
		List<String> pCList = input.pCList;
		String priTable = input.priTable;
		try {
			if(conn!=null)
				JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Connection Established");
//			ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + priTable);
			String col1, sql1, sql2;
			for (int j = 0; j < pCList.size(); j++) {
				col1 = pCList.get(j);
				sql1 = "select `" + col1 + "` from " + priTable;
				GetResults complex = obj.constructTrie(conn.createStatement().executeQuery(sql1), col1);
				trie = complex.trie;
				size = complex.size;
				for (Map.Entry entry : sec.entrySet()) {
					String sTab = (String) entry.getKey();
					List<String> sCols = (List<String>) entry.getValue();
					for( String col2 : sCols) {
						sql2 = "select `" + col2 + "` from " + sTab;
						pct =obj.findMatches(trie, conn.createStatement().executeQuery(sql2), size, col2);
						if (pct != -999)
							JobLogger.getLogger().info(Optimised.class.getName(), "main method",priTable + "." + col1 + " " + sTab + "." + col2
									+ " " + " = " + pct + "%");
					
					}
				}
				}
			}catch (Exception e) {
			JobLogger.getLogger().info(Optimised.class.getName(),"main method",e.getMessage());
		}
		Duration timeElapsed = Duration.between(start, Instant.now());
		JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Time elapsed "+timeElapsed);
		System.out.println("Done");
	}
}
