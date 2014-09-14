package com.taobao.tddl.repo.demo.spi;

import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.IDataSource;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.IConnectionHolder;
import com.taobao.tddl.executor.spi.ITransaction;

/**
 * @author danchen
 */
public class DemoTransaction implements ITransaction {

    private ExecutionContext executionContext;

    @Override
    public long getId() {
        return 0;
    }

    @Override
    public void commit() throws TddlException {
    }

    @Override
    public void rollback() throws TddlException {
    }

    @Override
    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;

    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    @Override
    public IConnectionHolder getConnectionHolder() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public IConnection getConnection(String groupName, IDataSource ds, RW rw) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isClosed() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void kill() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void cancel() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void tryClose() throws SQLException {
        // TODO Auto-generated method stub

    }

}
