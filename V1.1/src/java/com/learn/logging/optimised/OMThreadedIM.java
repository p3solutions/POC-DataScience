package com.learn.logging.optimised;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.learn.logging.logger.JobLogger;

class OMThreadedIM{
//	Connection setConnection(String url, String user ,String password) {
//		//check for mysql and mssql and return connection objects accordingly
//		int i =1;
//		try {
//			if(i == 1) {
//				Class.forName("com.mysql.jdbc.Driver");
//				return DriverManager.getConnection(url, user, password);				
//			}
//			else {
//	        	Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
//	            return DriverManager.getConnection("jdbc:sqlserver://34.213.4.182:57997;databaseName=PS_FINANCE", "sa", "secret@P3");
//
//			}
//		}catch (Exception e) {
//			JobLogger.getLogger().info(Optimised.class.getName(),"setConnection",e.getMessage());
//			return null;
//		}
//	}

	public GetResults constructTrie(ResultSet rs1, String col1) throws SQLException {
//		JobLogger.getLogger().info(Optimised.class.getName(), "constructTrie", "Before creating trie");
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
//		JobLogger.getLogger().info(Optimised.class.getName(), "constructTrie", "After creating trie");

		return(o);		
	}
	@SuppressWarnings("rawtypes")
	public void findMatches(Trie trie, ResultSet rs, int size, String col2, String table2, String tab1, String col1) throws SQLException{
		HashMap<String, Boolean> results = new HashMap<>();
		int count = 0;
//		JobLogger.getLogger().info(Optimised.class.getName(), "findMatches method", "Comparison for secondary column started");
		
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
//		if(size!=0)
//			return (count * 100 / size);
//		else
//			return -999;
		if(size == 0)
			JobLogger.getLogger().info(OMThreadedIM.class.getName(), "OUTSIDE", tab1+"."+col1+" "+table2+"."+col2  + "NA");
		else
//			System.out.println(tab1+"."+col1+" "+table2+"."+col2  + (count * 100 / size) + "%");
			JobLogger.getLogger().info(OMThreadedIM.class.getName(), "OUTSIDE", tab1+"."+col1+" "+table2+"."+col2  + (count * 100 / size) + "%");
//		JobLogger.getLogger().info(Optimised.class.getName(), "main method", tableList.get(i) + "." + col1 + " " + tableList.get(k) + "." + col2
//				+ " " + " = " + count * 100 / temp + "%");

		
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
//		System.out.println("\n" + sqlQuery);
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
}
