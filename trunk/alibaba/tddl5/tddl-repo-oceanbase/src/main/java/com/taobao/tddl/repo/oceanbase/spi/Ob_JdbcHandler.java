package com.taobao.tddl.repo.oceanbase.spi;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;

public class Ob_JdbcHandler extends My_JdbcHandler {

    private static Method getBaseConnection = null;
    private static Method realClose         = null;

    public Ob_JdbcHandler(ExecutionContext executionContext){
        super(executionContext);
    }

    /**
     * Ob不支持流模式
     */
    // @Override
    // protected void setStreamingForStatement(Statement stat) throws
    // SQLException {
    // return;
    // }
    //
    // @Override
    // protected void setContext(ICursorMeta meta, boolean isStreaming) {
    // super.setContext(meta, false);
    // }
    //
    // @Override
    // public void setIsStreaming(Boolean streaming) {
    // super.setIsStreaming(false);
    // }

    @Override
    protected boolean closeStreaming(Statement stmt, ResultSet rs) throws SQLException {

        if (!this.executionContext.isAutoCommit()) {
            rs.close();
            return false;
        }

        Connection conn = stmt.getConnection();
        Object obj = null;
        try {
            if (getBaseConnection == null) {
                getBaseConnection = conn.getClass().getDeclaredMethod("getBaseConnection",
                    new Class[] { String.class, boolean.class });
                getBaseConnection.setAccessible(true);
            }

            obj = getBaseConnection.invoke(conn, new Object[] { "", true });
            if (obj == null) {
                obj = getBaseConnection.invoke(conn, new Object[] { "", false });
            }
        } catch (Throwable e) {
            // do nothing
        }
        try {
            // 物理关闭
            Connection real = ((Connection) obj).unwrap(Connection.class);

            if (realClose == null) {
                realClose = real.getClass()
                    .getSuperclass()
                    .getDeclaredMethod("realClose",
                        new Class[] { boolean.class, boolean.class, boolean.class, Throwable.class });
                realClose.setAccessible(true);
            }

            realClose.invoke(real, new Object[] { false, false, true, null });

            return true;
        } catch (Throwable e) {
            e.printStackTrace();
        }

        // 忽略异常，直接调用resultSet.close
        rs.close();
        return false;

    }

    /**
     * 为ob搞的，ob不支持bigdecimal，统一转double
     * 
     * @param params
     */
    @Override
    public void convertBigDecimal(Map<Integer, ParameterContext> params) {
        for (ParameterContext paramContext : params.values()) {
            Object value = paramContext.getValue();
            if (value instanceof BigDecimal) {
                value = ((BigDecimal) value).doubleValue();
                paramContext.setValue(value);
            }
        }
    }

    @Override
    protected String buildTraceComment() {
        return null;
    }

}
