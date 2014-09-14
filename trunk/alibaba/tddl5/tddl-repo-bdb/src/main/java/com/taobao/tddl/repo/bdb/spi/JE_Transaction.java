package com.taobao.tddl.repo.bdb.spi;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.IDataSource;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.IConnectionHolder;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.spi.ITHLog;
import com.taobao.tddl.executor.spi.ITransaction;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class JE_Transaction implements ITransaction {

    final AtomicReference<ITHLog> historyLog;
    Transaction                   txn;
    TransactionConfig             config;
    List<Cursor>                  openedCursors = new LinkedList<Cursor>();

    public JE_Transaction(com.sleepycat.je.Transaction txn, com.sleepycat.je.TransactionConfig config,
                          AtomicReference<ITHLog> historyLog){
        this.txn = txn;
        this.config = config;
        this.historyLog = historyLog;
    }

    @Override
    public long getId() {
        if (txn == null) {
            throw new IllegalArgumentException("事务为空");
        }
        return txn.getId();
    }

    @Override
    public void commit() throws TddlException {
        try {
            closeCursor();
            txn.commit();
            try {
                ITHLog ithLog = historyLog.get();
                if (ithLog != null) {
                    ithLog.commit(this);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } catch (ReplicaWriteException replicaWrite) {
            try {
                rollback();
            } catch (Exception throwable) {
                throw new TddlNestableRuntimeException(throwable);
            }
        }
    }

    @Override
    public void rollback() throws TddlException {
        if (txn != null) {
            closeCursor();
            txn.abort();
            txn = null;
            try {
                ITHLog ithLog = historyLog.get();
                if (ithLog != null) {
                    ithLog.rollback(this);
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void closeCursor() throws TddlException {
        List<TddlException> ex = new ArrayList();
        for (Cursor cursor : openedCursors) {
            ex = cursor.close(ex);
        }
        if (!ex.isEmpty()) {
            throw new TddlNestableRuntimeException(GeneralUtil.mergeException(ex));
        }

    }

    public void addCursor(Cursor cursor) {
        openedCursors.add(cursor);
    }

    public List<Cursor> getCursors() {
        return openedCursors;
    }

    @Override
    public void close() {
        throw new IllegalArgumentException("not supported yet");
    }

    @Override
    public void setExecutionContext(ExecutionContext executionContext) {
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
