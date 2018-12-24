package com.learn.logging.optimised;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ThreadPoolExecutor;

import com.learn.logging.logger.JobLogger;

class OMThreadedThread extends Thread{
//	ArrayList<String> secondaryColNames;
	String table;
	Connection conn;
	Trie trie;
	int size;
	String tab1;
	String col1, col2;
	CPDemo obj1;
//	String threadName;
//	public OMThreadedThread(ArrayList<String> secondaryColNames, String table, Trie trie, int size, String tab1, String col1) throws ClassNotFoundException, SQLException {
	public OMThreadedThread(String col2, String table, Trie trie, int size, String tab1, String col1, CPDemo obj1) throws ClassNotFoundException, SQLException {

//		this.secondaryColNames = secondaryColNames;
		this.table = table;
//		this.conn = conn;
		this.trie = trie;
		this.size = size;
		this.col1 = col1;
		this.col2 = col2;
		this.tab1 = tab1;
		this.obj1 = obj1;
//		String url = "jdbc:mysql://localhost:3306/sakila",user = "root",password = "secret";
//		Class.forName("com.mysql.jdbc.Driver");
//		conn = DriverManager.getConnection(url, user, password);
//        conn = connectionPool.getConnection();
	}
	
	public void run() {
		String sql2;
		ResultSet rss = null;
		OMThreadedIM obj;
//		for(int l =0; l < secondaryColNames.size(); l++) {
//			col2 = secondaryColNames.get(l);
			sql2 = "select `" + col2 + "` from " + table;
			obj = new OMThreadedIM();
			try {
				conn =obj1.getConnectionFRomPool();
				rss = obj.getRecords(conn, sql2);
				obj.findMatches(trie, rss, size, col2, table, tab1, col1);
				rss.close();
//				if (pct != -999)
//					JobLogger.getLogger().info(Optimised.class.getName(), "main method", tableList.get(i) + "." + col1 + " " + tableList.get(k) + "." + col2
//							+ " " + " = " + pct + "%");
//					JobLogger.getLogger().info(Optimised.class.getName(), "main method",  + pct + "%");

//					System.out.println(pct);

			} catch (SQLException e) {
				e.printStackTrace();
			}
		obj1.connectionPool.free(conn);
	}
//	public void setCurrentThreadName(String name) {
//		this.threadName = name;		
//	}
}

