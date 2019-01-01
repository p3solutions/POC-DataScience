package com.learn.logging.optimised;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class GenerateMetadata {
	static Connection conn;

	public GenerateMetadata(Connection conn) throws SQLException, ClassNotFoundException {
//		String url = "jdbc:mysql://localhost:3306/sakila";
//		String user = "root";
//		String password = "secret";
//		Class.forName("com.mysql.jdbc.Driver");
//		conn = DriverManager.getConnection(url, user, password);
		this.conn = conn;
	}

	@SuppressWarnings("unchecked")
	public void generate() throws ClassNotFoundException, SQLException, FileNotFoundException {
		Map<String, String> hm = new HashMap<String, String>();
		String t;
		OMThreadedIM selfObj = new OMThreadedIM();
		ResultSet rst = conn.getMetaData().getTables(null, null, "%", null);
		int rowCount = 0;
		if (conn != null)
			System.out.println("Established connection");
		JSONObject jo = new JSONObject(), k = new JSONObject(), f = new JSONObject();

		JSONArray jao = null;
		PrintWriter pw = null;
		Map element = null;
		pw = new PrintWriter("md.json");
		while (rst.next()) {
			
			element = new LinkedHashMap(2);
			t = rst.getString(3);
			jao = new JSONArray();
			String stmt = "SELECT * FROM " + t;
			ResultSet rs = selfObj.getRecords(conn, stmt);
			hm = selfObj.getColumnNames(rs);
			rowCount = selfObj.getRowCount(conn, t);
			element.put("columns", hm);
			element.put("rowcount", rowCount);
			k.put(t, element);
			jao.add(k);
		}
		jo.put("table", k);	
		pw.write(jo.toJSONString());

		pw.flush();
		pw.close();

	}
}
