package com.learn.logging.optimised;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.commons.dbcp2.BasicDataSource;

class OMThreadedThread extends Thread{
	ArrayList<String> secondaryColNames;
	String table;
	Connection conn;
	Trie trie;
	int size;

	public OMThreadedThread(ArrayList<String> secondaryColNames, String table, Trie trie, int size) throws ClassNotFoundException, SQLException {
		this.secondaryColNames = secondaryColNames;
		this.table = table;
//		this.conn = conn;
		this.trie = trie;
		this.size = size;
		String url = "jdbc:mysql://localhost:3306/employees",user = "root",password = "secret";
		Class.forName("com.mysql.jdbc.Driver");
		conn = DriverManager.getConnection(url, user, password);				


	}
	public void run() {
		String col2, sql2;
		int pct;
		ResultSet rss;
		OMThreadedIM obj;
		for(int l =0; l < secondaryColNames.size(); l++) {
			col2 = secondaryColNames.get(l);
			sql2 = "select `" + col2 + "` from " + table;
			obj = new OMThreadedIM();
			try {
				rss = obj.getRecords(conn, sql2);
				pct = obj.findMatches(trie, rss, size, col2);
				rss.close();
				if (pct != -999)
//					JobLogger.getLogger().info(Optimised.class.getName(), "main method", tableList.get(i) + "." + col1 + " " + tableList.get(k) + "." + col2
//							+ " " + " = " + pct + "%");
					System.out.println(pct);

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}
}

