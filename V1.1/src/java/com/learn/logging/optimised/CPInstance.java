
package com.learn.logging.optimised;

import java.sql.Connection;
import java.sql.SQLException;
 
public class CPInstance {
	ConnectionPool connectionPool;
	
	public CPInstance() throws SQLException {
//        connectionPool = new ConnectionPool(
//                "com.mysql.jdbc.Driver",
//                "jdbc:mysql://localhost:3306/sakila", "root", "secret",
//                5, 10, true);

        connectionPool = new ConnectionPool(
                "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "jdbc:sqlserver://34.213.4.182:57997;databaseName=PS_FINANCE", "sa", "secret@P3",
                5, 50, true);
	}
	public Connection getConnectionFRomPool() throws SQLException {
		
        Connection conn = connectionPool.getConnection();
        return conn;
	}
}
 