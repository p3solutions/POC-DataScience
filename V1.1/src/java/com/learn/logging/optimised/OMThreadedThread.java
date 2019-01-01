package com.learn.logging.optimised;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

class OMThreadedThread extends Thread{
//	ArrayList<String> secondaryColNames;
	String table;
	Connection conn;
	Trie trie;
	int size;
	String tab1;
	String col1, col2;
	CPDemo obj1;
	String[] c1;
//	String threadName;
	public OMThreadedThread(String col2, String table, Trie trie, int size, String tab1, String col1, CPDemo obj1) throws ClassNotFoundException, SQLException {
//	public OMThreadedThread(String col2, String table, int size, String tab1, String col1, CPDemo obj1, String[] c1) throws ClassNotFoundException, SQLException {

//		this.secondaryColNames = secondaryColNames;
		this.table = table;
//		this.conn = conn;
		this.trie = trie;
		this.size = size;
		this.col1 = col1;
		this.col2 = col2;
		this.tab1 = tab1;
		this.obj1 = obj1;
//		this.c1 = c1;
	}
	
	public void run() {
		String sql2;
		ResultSet rss = null;
		OMThreadedIM obj;
//		for(int l =0; l < secondaryColNames.size(); l++) {
//			col2 = secondaryColNames.get(l);
			sql2 = "select `" + col2 + "` from " + table;
//			sql2 = "select " + col2 + " from " + table;
			obj = new OMThreadedIM();
			try {
				conn =obj1.getConnectionFRomPool();
				rss = obj.getRecords(conn, sql2);
				obj.findMatches(trie, rss, size, col2, table, tab1, col1);
				rss.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		obj1.connectionPool.free(conn);
	}
//	public void setCurrentThreadName(String name) {
//		this.threadName = name;		
//	}


	/*
	public void run() {
		String sql2;
		ResultSet rss = null;
		OMThreadedIM obj;
//		for(int l =0; l < secondaryColNames.size(); l++) {
//			col2 = secondaryColNames.get(l);
			sql2 = "select `" + col2 + "` from " + table;
//			sql2 = "select " + col2 + " from " + table;
			obj = new OMThreadedIM();
			try {
				conn =obj1.getConnectionFRomPool();
				rss = obj.getRecords(conn, sql2);
				String[] c2 = obj.getCol2(rss, col2);
				int count = obj.compareNLogN(c1, c2);
				obj.generateResult(tab1, col1, table, col2, size, count);
				rss.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		obj1.connectionPool.free(conn);
	}


*/
}

