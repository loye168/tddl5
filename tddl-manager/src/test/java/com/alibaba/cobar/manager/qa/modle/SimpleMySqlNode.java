package com.alibaba.cobar.manager.qa.modle;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.jdbc.Connection;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class SimpleMySqlNode {

    private static final Logger logger = LoggerFactory.getLogger(SimpleMySqlNode.class);
    private static String       Driver = "com.mysql.jdbc.Driver";
    private String              ip     = null;

    static {
        try {
            Class.forName(Driver);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    public SimpleMySqlNode(String ip){
        this.ip = ip;
    }

    public Connection createConnection(int port, String user, String password, String schema) throws Exception {
        Connection conn = null;
        conn = (Connection) DriverManager.getConnection("jdbc:mysql://" + ip + ":" + port + "/" + schema,
            user,
            password);
        return conn;
    }

    public boolean detoryConnection(java.sql.Connection conn) {
        boolean success = false;
        if (null == conn) {
            success = true;
        } else {
            try {
                conn.close();
                success = true;
            } catch (Exception e2) {
                logger.error(e2.getMessage(), e2);
            }
        }
        return success;
    }

    public void executeSQLRead(java.sql.Connection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery(sql);
        } finally {
            // only catch exceptions when stmt and rs closed
            try {
                rs.close();
                stmt.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    public int excuteSQWrite(java.sql.Connection dmlConnection, String sql) throws SQLException {
        Statement stmt = dmlConnection.createStatement();
        int result = 0;
        try {
            result = stmt.executeUpdate(sql);
        } finally {
            // only catch exceptions when stmt closed
            try {
                stmt.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        return result;
    }

    public boolean excuteSQL(java.sql.Connection managerConnection, String sql) throws SQLException {
        Statement stmt = managerConnection.createStatement();
        boolean success = false;
        try {
            stmt.execute(sql);
            success = true;
        } finally {
            try {
                stmt.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        return success;
    }
}
