package com.learn.logging.optimised;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.learn.logging.logger.JobLogger;

public class Optimised {
	public static void main(String[] args) {
		
		JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Start");
		String url = "jdbc:mysql://localhost:3306/sakila";
		String user = "root";
		String password = "secret";
		List<String> toFind = new ArrayList<>();
		final List<String> tableList = new ArrayList<>();
		HashMap<String, Boolean> results = new HashMap<>();
		int count, temp;
		Trie trie;

		System.out.println("Started");
		Instant start = Instant.now();

		try {
//        	Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
//            Connection conn = DriverManager.getConnection("jdbc:sqlserver://34.213.4.182:57997;databaseName=PS_FINANCE", "sa", "secret@P3");
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url, user, password);
			if (conn != null)
				JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Connection Established");

			ResultSet rst = conn.getMetaData().getTables(null, null, "%", null);

			String col1, col2, sql1, sql2;
			while (rst.next())
				tableList.add(rst.getString(3));
			JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Table List obtained");
			rst = null;

			for (int i = 0; i < tableList.size(); i++) {
				// System.out.println("Primary table " + tableList.get(i));
				ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableList.get(i));

				// ResultSetMetaData rsmd1 = rs.getMetaData();
				for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
					col1 = rs.getMetaData().getColumnName(j + 1);
					JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Primary Column selected");

					sql1 = "select `" + col1 + "` from " + tableList.get(i);

					ResultSet rs1 = conn.createStatement().executeQuery(sql1);
					temp = 0;
					JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Before creating trie");

					trie = new Trie();
					while (rs1.next()) {
						temp++;
						if (rs1.getString(col1) == null)
							continue;
						trie.insert(rs1.getString(col1));
					}
					rs1 = null;
					JobLogger.getLogger().info(Optimised.class.getName(), "main method", "After creating trie");

					for (int k = i + 1; k < tableList.size(); k++) {

						ResultSet rss = conn.createStatement().executeQuery("SELECT * FROM " + tableList.get(k));

						for (int l = 0; l < rss.getMetaData().getColumnCount(); l++) {
							col2 = rss.getMetaData().getColumnName(l + 1);
							JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Secondary column selected");

							sql2 = "select `" + col2 + "` from " + tableList.get(k);
							
							ResultSet rs2 = conn.createStatement().executeQuery(sql2);
							toFind = new ArrayList<>();
							JobLogger.getLogger().info(Optimised.class.getName(), "main method", "After crreating trie");
							results = new HashMap<>();

							while (rs2.next()) {
								String param2 = rs2.getString(col2);
								if (param2 != null)
//								toFind.add(param2 + "");
								results.put(param2, trie.search(param2));

							}
//							results = new HashMap<>();
							rs2 = null;
							JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Comparison for secondary column started");

//							for (String s : toFind) {
//								results.put(s, trie.search(s));
//							}
							count = 0;

							for (Map.Entry entry : results.entrySet()) {
								if ((Boolean) entry.getValue() == true) {
									count++;
								}
							}
//							count = 0;
//							for (String s : toFind) {
//								if(trie.search(s))
//									count++;
//							}
							
							JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Comparison for secondary column ended");

							if (temp != 0)
								JobLogger.getLogger().info(Optimised.class.getName(), "main method", tableList.get(i) + "." + col1 + " " + tableList.get(k) + "." + col2
										+ " " + " = " + count * 100 / temp + "%");

//							toFind = new ArrayList<>();
//							System.gc();
						}
						rss = null;
					}
				}
			}
		} catch (Exception e) {
			JobLogger.getLogger().info(Optimised.class.getName(),"main method",e.getMessage());
		}
		Instant end = Instant.now();
		Duration timeElapsed = Duration.between(start, end);
		JobLogger.getLogger().info(Optimised.class.getName(), "main method", "Time elapsed "+timeElapsed);
		System.out.println("Done");
	}
}
