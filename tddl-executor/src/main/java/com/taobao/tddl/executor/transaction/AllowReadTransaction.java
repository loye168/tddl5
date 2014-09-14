package com.taobao.tddl.executor.transaction;

import java.sql.SQLException;
import java.util.Set;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.IDataSource;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.IConnectionHolder;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

/**
 * @author mengshi.sunmengshi 2013-12-6 上午11:31:29
 * @since 5.0.0
 */
public class AllowReadTransaction extends BaseTransaction {

    protected final static Logger logger          = LoggerFactory.getLogger(AllowReadTransaction.class);

    /**
     * 当前进行事务的节点
     */
    protected String              writeGroupName  = null;
    protected IConnection         writeConnection = null;
    private IConnectionHolder     ch;

    private IConnectionHolder     readConnectionHolder;
    private IConnectionHolder     writeConnectionHolder;

    public AllowReadTransaction(ExecutionContext ec){
        super(ec);
        readConnectionHolder = new AutoCommitConnectionHolder();
        writeConnectionHolder = new StrictConnectionHolder();

        this.ch = new ConnectionHolderCombiner(readConnectionHolder, writeConnectionHolder);
    }

    /**
     * 策略两种：1. 强一致策略，事务中不允许跨机查询。2.弱一致策略，事务中允许跨机查询；
     * 
     * @param groupName
     * @param ds
     * @param strongConsistent 这个请求是否是强一致的，这个与ALLOW_READ一起作用。
     * 当ALLOW_READ的情况下，strongConsistent =
     * true时，会创建事务链接，而如果sConsistent=false则会创建非事务链接
     * @return
     */
    @Override
    public IConnection getConnection(String groupName, IDataSource ds, RW rw) throws SQLException {
        if (groupName == null) {
            throw new IllegalArgumentException("group name is null");
        }

        lock.lock();

        try {
            if (rw == RW.READ) {
                if (groupName.equals(writeGroupName)) {
                    return this.writeConnectionHolder.getConnection(groupName, ds);
                }
                return readConnectionHolder.getConnection(groupName, ds);
            }

            IConnection conn = null;
            if (writeGroupName == null) {
                writeGroupName = groupName;
            }
            if (writeGroupName != null) {// 已经有事务链接了
                if (writeGroupName.equalsIgnoreCase(groupName)
                    || IDataNodeExecutor.USE_LAST_DATA_NODE.equals(groupName)) {
                    conn = this.writeConnectionHolder.getConnection(writeGroupName, ds);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_ACCROSS_DB_TRANSACTION, writeGroupName, groupName);
                }

                conn.setAutoCommit(false);
                this.writeConnection = conn;
            }
            return conn;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit() throws TddlException {

        lock.lock();

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("commit");
            }
            Set<IConnection> conns = this.writeConnectionHolder.getAllConnection();

            for (IConnection conn : conns) {
                try {
                    conn.commit();
                } catch (SQLException e) {
                    throw new TddlNestableRuntimeException(e);
                }
            }

            close();
        } finally {
            lock.unlock();
        }

    }

    @Override
    public void rollback() throws TddlException {
        lock.lock();

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("rollback");
            }
            Set<IConnection> conns = this.writeConnectionHolder.getAllConnection();

            for (IConnection conn : conns) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    throw new TddlNestableRuntimeException(e);
                }
            }

            close();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public IConnectionHolder getConnectionHolder() {
        return this.ch;
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        lock.lock();

        try {
            if (conn == this.writeConnection) {
                this.writeConnectionHolder.tryClose(conn, groupName);
            } else {
                this.readConnectionHolder.tryClose(conn, groupName);
            }
        } finally {
            lock.unlock();
        }

    }

    @Override
    public void close() {

        if (isClosed()) {
            return;
        }

        lock.lock();

        try {
            super.close();

            this.writeConnectionHolder.closeAllConnections();
            this.readConnectionHolder.closeAllConnections();
        } finally {
            lock.unlock();
        }

    }

    @Override
    public void tryClose() throws SQLException {
        if (isClosed()) {
            return;
        }

        lock.lock();

        try {

            this.readConnectionHolder.closeAllConnections();

        } finally {
            lock.unlock();
        }

    }

}
