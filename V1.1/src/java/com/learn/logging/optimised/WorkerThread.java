package com.learn.logging.optimised;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

class WorkerThread extends Thread {
	String table;
	Connection conn;
	Trie trie;
	int size;
	String tab1;
	String col1, col2;
	CPInstance obj1;
	String[] c1;

    public WorkerThread(String col2, String table, Trie trie, int size, String tab1, String col1, CPInstance obj1) throws ClassNotFoundException, SQLException{
    	this.table = table;
    	this.trie = trie;
    	this.size = size;
    	this.col1 = col1;
    	this.col2 = col2;
    	this.tab1 = tab1;
    	this.obj1 = obj1;    
    	}
 
    public void run() {
    	String sql2;
    	ResultSet rss = null;
    	OMThreadedIM obj;
//    		sql2 = "select `" + col2 + "` from " + table;
    		sql2 = "select " + col2 + " from " + table;
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
 
   
}

