package com.ng.dataanalysis2.util;
import java.sql.*;

public class JDBCUtil {
    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URI = "jdbc:mysql://psy831:3306/dbtest?useUnicode=true&characterEncoding=UTF8";
    private static final String USER_NAME = "root";
    private static final String PASSWORD = "root";
    private static Connection conn = null;

    public static Connection getInstance() {
        if (conn == null) {
            synchronized (JDBCUtil.class) {
                if (conn == null) {
                    conn = getConnection();
                }
            }
        }
        return conn;
    }

    private static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(DRIVER);
            conn = DriverManager.getConnection(URI, USER_NAME, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void close(ResultSet set, PreparedStatement ps, Connection conn) {
        if(set != null){
            try {
                set.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if(ps != null){
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

