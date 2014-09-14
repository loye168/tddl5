package com.taobao.tddl.repo.hbase.spi;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.IDataSource;
import com.taobao.tddl.executor.common.AtomicNumberCreator;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.IConnectionHolder;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.spi.ITransaction;

/**
 * Hbase自身事务支持比较弱，不支持rollback/commit语义
 * 
 * <pre>
 * 目前hbase版本事务支持的很有限，0.94版本中支持的事务特性： 
 *   a.  [HBASE-3584] - Allow atomic put/delete in one call.
 *   b.  [HBASE-5229] - Provide basic building blocks for "multi-row" local transactions.
 * 基本上可以理解为只能保证单机事务，多机写操作无法保证事务性，和mysql的事务支持的功能有点
 * 
 * Region的实现：{@linkplain MultiRowMutationEndpoint}，Client单Row事务：{@linkplain RowMutations}
 * Client跨Row事务支持：http://hadoop-hbase.blogspot.com/2012/02/limited-cross-row-transactions-in-hbase.html
 * </pre>
 * 
 * @author jianghang 2013-7-26 上午11:30:42
 * @since 3.0.1
 */
public class HbTransaction implements ITransaction {

    private final AtomicNumberCreator idGen = AtomicNumberCreator.getNewInstance();
    private ExecutionContext          executionContext;

    @Override
    public long getId() {
        return idGen.getLongNextNumber();
    }

    public void addCursor(Cursor cursor) {
    }

    public List<Cursor> getCursors() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;

    }

    @Override
    public void commit() throws TddlException {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollback() throws TddlException {
        // TODO Auto-generated method stub

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
