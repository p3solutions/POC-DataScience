package com.learn.logging.optimised;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
	

	public GetResults1 getCol1(ResultSet rs1, String col1) throws SQLException {
//		JobLogger.getLogger().info(Optimised.class.getName(), "constructTrie", "Before creating trie");
		int size = 0;
		ArrayList<String> al = new ArrayList<>();
		while (rs1.next()) {
			size++;
			if (rs1.getString(col1) == null)
				continue;
			al.add(rs1.getString(col1)+"");
		}
		rs1 = null;
		GetResults1 o = new GetResults1();
		String[] k = new String[al.size()];
		for(int i = 0; i<al.size();i++)
			k[i] = al.get(i);

		o.col1 = k;
		o.size = size;
//		JobLogger.getLogger().info(Optimised.class.getName(), "constructTrie", "After creating trie");

		return(o);		
	}
	public String[] getCol2(ResultSet rs1, String col2) throws SQLException {
//		JobLogger.getLogger().info(Optimised.class.getName(), "constructTrie", "Before creating trie");
		ArrayList<String> al = new ArrayList<>();
		while (rs1.next()) {
			if (rs1.getString(col2) == null)
				continue;
			al.add(rs1.getString(col2));
		}
		rs1 = null;
		String[] k = new String[al.size()];
		for(int i = 0; i<al.size();i++)
			k[i] = al.get(i);

	
		return k;		
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

		if(size == 0)
			JobLogger.getLogger().info(OMThreadedIM.class.getName(), "OUTSIDE", tab1+"."+col1+" "+table2+"."+col2  + " NA");
		else
			JobLogger.getLogger().info(OMThreadedIM.class.getName(), "findMatches", tab1+"."+col1+" "+table2+"."+col2  + "  "+(count * 100 / size) + "%");

		
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
//		System.out.println("SQL2 : " + sqlQuery);
		PreparedStatement sql = conn.prepareStatement(sqlQuery,ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY );
//		sql.setFetchSize(Integer.MIN_VALUE); 
		ResultSet rs = sql.executeQuery();
		return rs;
	}
/*	public ArrayList<String> getColumnNames(ResultSet rs)throws SQLException {
		ArrayList<String> colNames = new ArrayList<String>();
		ResultSetMetaData rsmd = rs.getMetaData();
		String type;
		for (int j = 1; j < rsmd.getColumnCount(); j++) {
			type = rsmd.getColumnTypeName(j);
			if(isallowedtype(type)==true)
				colNames.add(rsmd.getColumnName(j));
		}
		return colNames;
	}
	*/
	
	public Map getColumnNames(ResultSet rs)throws SQLException {
		ArrayList<String> colNames = new ArrayList<String>();
		Map tuple = new HashMap<String, String>();
		ResultSetMetaData rsmd = rs.getMetaData();
		String type;
		for (int j = 1; j < rsmd.getColumnCount(); j++) {
			type = rsmd.getColumnTypeName(j);
			if(isallowedtype(type)==true) {
				colNames.add(rsmd.getColumnName(j));
				tuple.put(rsmd.getColumnName(j), type);}
		}
		return tuple;
	}
	public int getRowCount(Connection conn, String t) throws SQLException {
		ResultSet r = conn.createStatement().executeQuery("SELECT COUNT(*) AS rowcount FROM "+t);
		r.next();
		int count = r.getInt("rowcount");
		r.close();
		return count;
	}

	public int compareNLogN(String col1[], String col2[]) {
		/*int count = 0, comparison;
		Arrays.sort(col1); Arrays.sort(col2);
		int leftPointer = 0, rightPointer = 0;
		while(leftPointer<col1.length && rightPointer<col2.length) {
			comparison = col1[leftPointer].compareTo(col2[rightPointer]);
			if(comparison == 0) {
				count++;
				leftPointer++;
				rightPointer++;
			}else if(comparison < 0)
				leftPointer++;
			else
				rightPointer++;
		}*/
        int ptr1=0, ptr2=0, len1=col1.length, len2=col2.length;
        int count = 0;
        while((ptr1 < len1) && (ptr2 < len2) ){
            int out = col1[ptr1].compareTo(col2[ptr2]);
            if (out < 0) {
                ptr1++;
            }
            else if (out > 0) {
                ptr2++;
            }
            else{
                count++;
                ptr2++;
            }
        }

			return count;
	}

	public void generateResult(String table1, String col1, String table2, String col2, int size, int count) {
		System.out.println(table1+"."+col1+" "+table2+"."+col2  + (count * 100 / size) + "%");
	}
	
	public boolean isallowedtype(String DataType) {
		switch (DataType.toUpperCase()) {
		case "BLOB":
		case "TINYBLOB":
		case "MEDIUMBLOB":
		case "LONGBLOB":
		case "VARBINARY":
		case "NVARBINARY":
		case "IMAGE":
		case "PHOTO":
		case "BIT":
		case "BOOLEAN":
		case "MONEY":
		case "CURRENCY":
		case "SMALLMONEY":
		case "BINARY_BOUBLE":
		case "BINARY_FLOAT":
		case "DOUBLE_PRECISION":
		case "DOUBLE PRECISION":
		case "BINARY VARYING":
		case "TIME":
		case "TIMESTAMP":
		case "INTERVAL":
		case "TIMESTAMP WITH LOCAL TIME ZONE":
		case "TIMESTAMP WITH TIME ZONE":
		case "DATETIME":
		case "DATETIME2":
		case "SMALLDATETIME":
		case "DATETIMEOFFSET":
		case "DATE":
		case "RAW":
		case "LONG RAW":
			return false;
		default:
			return true;
		}
	}
}







