package com.taobao.tddl.executor.spi;

import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.IDataSource;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.IConnectionHolder;

/**
 * 事务对象
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午4:00:49
 * @since 5.0.0
 */
public interface ITransaction {

    public enum RW {
        READ, WRITE
    }

    long getId();

    void commit() throws TddlException;

    void rollback() throws TddlException;

    void setExecutionContext(ExecutionContext executionContext);

    IConnectionHolder getConnectionHolder();

    void tryClose(IConnection conn, String groupName) throws SQLException;

    void tryClose() throws SQLException;

    IConnection getConnection(String groupName, IDataSource ds, RW rw) throws SQLException;

    boolean isClosed();

    void close();

    void kill() throws SQLException;

    void cancel() throws SQLException;

}
