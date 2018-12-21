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
import java.sql.PreparedStatement;


public class OptimisedMod1 {
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
//		Map m = new HashMap<Trie, Integer>();
//		List<Object> l = new ArrayList<>();
		GetResults o = new GetResults();
		o.trie =trie;
		o.size = size;
//		l.add(trie);
//		l.add(size);
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
//			toFind.add(param2 + "");
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
	
	public ResultSet getRecords(Connection conn, String sqlQuery) throws SQLException{
		System.out.println("\n" + sqlQuery);
		PreparedStatement sql = conn.prepareStatement(sqlQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY );
		sql.setFetchSize(Integer.MIN_VALUE); 
		ResultSet rs = sql.executeQuery();
		
		return rs;
	}
	public ArrayList<String> getColumnNames(ResultSet rs)throws SQLException {
		
		ArrayList<String> colNames = new ArrayList<String>();
		for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
			colNames.add(rs.getMetaData().getColumnName(j + 1));
		}

		return colNames;
	}
	
	public static void main(String[] args) {
		OptimisedMod1 obj = new OptimisedMod1();
		JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Start");
		String url = "jdbc:mysql://localhost:3306/employees",user = "root",password = "secret";
		Connection conn = obj.setConnection(url, user, password);
		final List<String> tableList = new ArrayList<>();
		int size, pct;
		Trie trie;
		System.out.println("Started");
		Instant start = Instant.now();

		try {
			if(conn!=null)
				JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Connection Established");
			ResultSet rst = conn.getMetaData().getTables(null, null, "%", null);
			String col1, col2, sql1, sql2;
			while (rst.next()) {
				tableList.add(rst.getString(3));
				System.out.println(rst.getString(3));
			}
			
			JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Table List obtained");
			rst = null;

			for (int i = 0; i < tableList.size(); i++) {
				String stmt = "SELECT * FROM " + tableList.get(i);
				ResultSet rs = obj.getRecords(conn, stmt);
				ArrayList<String> colNames = obj.getColumnNames(rs);
				rs.close();
								
				//ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableList.get(i));
				
			//	for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
				for(int j = 0; j < colNames.size(); j++ ) {
					//col1 = rs.getMetaData().getColumnName(j + 1);
					col1 = colNames.get(j);
					System.out.println(col1);
					String colDataFetchQry = "select `" + col1 + "` from " + tableList.get(i);					
					//GetResults complex = obj.constructTrie(conn.createStatement().executeQuery(sql1), col1);
					rs = obj.getRecords(conn, colDataFetchQry);
				
				//	GetResults complex = obj.constructTrie(obj.getRecords(conn,colDataFetchQry), col1);
					GetResults complex = obj.constructTrie(rs, col1);
					trie = complex.trie;
					size = complex.size;
					rs.close();
					
					for (int k = i + 1; k < tableList.size(); k++) {
						//ResultSet rss = conn.createStatement().executeQuery("SELECT * FROM " + tableList.get(k));
						String secondaryTableQry = "SELECT * FROM " + tableList.get(k);
						rs = obj.getRecords(conn, secondaryTableQry);
						ArrayList<String> secondaryColNames = obj.getColumnNames(rs);
						rs.close();
						
						//for (int l = 0; l < rss.getMetaData().getColumnCount(); l++) {
						for(int l =0; l < secondaryColNames.size(); l++) {
//							obj.garbageCollect();
							//col2 = rss.getMetaData().getColumnName(l + 1);
							col2 = secondaryColNames.get(l);
							sql2 = "select `" + col2 + "` from " + tableList.get(k);
//							toFind = obj.makeSecondaryCol(conn.createStatement().executeQuery(sql2), col2);							
							rs = obj.getRecords(conn, sql2);
							//pct =obj.findMatches(trie, conn.createStatement().executeQuery(sql2), size, col2);
							pct = obj.findMatches(trie, rs, size, col2);
							rs.close();
							if (pct != -999)
								JobLogger.getLogger().info(Optimised.class.getName(), "main method", tableList.get(i) + "." + col1 + " " + tableList.get(k) + "." + col2
										+ " " + " = " + pct + "%");
						}
						//rss = null;
					}
					
										
				}
	

				
			}
			
		} catch (Exception e) {
			JobLogger.getLogger().info(Optimised.class.getName(),"main method",e.getMessage());
			System.out.println(e.getMessage());
		}
		Duration timeElapsed = Duration.between(start, Instant.now());
		JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Time elapsed "+timeElapsed);
		System.out.println("Done");
	}
}
