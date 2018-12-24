
package com.learn.logging.optimised;

import java.sql.Connection;
import java.sql.SQLException;
 
/** Copyright (c), AnkitMittal JavaMadeSoEasy.com */
public class CPDemo {
	ConnectionPool connectionPool;
	
	public CPDemo() throws SQLException {
        connectionPool = new ConnectionPool(
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/sakila", "root", "secret",
                5, 10, true);

	}
	public Connection getConnectionFRomPool() throws SQLException {
		
        Connection conn = connectionPool.getConnection();
//        System.out.println("We have got connection from ConnectionPool class");

        return conn;
	}
    public static void main(String[] arg) throws SQLException {
 
//           connectionPool.free(con);
//           System.out.println("We have free/released connection to ConnectionPool class");
    }
}
 