package com.ng.dataconsumer.util;

import java.sql.*;

public class JDBCUtil {
    private static final String DRIVER ="com.mysql.jdbc.Dirver";
    private static final String URI = "jdbc:mysql://psy831:3306/dbtest?useUnicode=true&characterEncoding=UTF8";
    private static final String USER_NAME = "root";
    private static final String PASSWORD = "root";
    private static Connection conn = null;

    /**
     * 单例模式
     * @return
     */
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

    /**
     * 获取 JDBC 连接
     * @return
     */
    private static Connection getConnection(){
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

    /**
     * 关闭资源
     * @param resultSet
     * @param preparedStatement
     * @param connection
     */
    public static void close(ResultSet set, PreparedStatement ps, Connection conn){
        if (set != null) {
            try {
                set.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

