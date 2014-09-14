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
public class StrictlTransaction extends BaseTransaction {

    protected final static Logger logger                = LoggerFactory.getLogger(StrictlTransaction.class);

    /**
     * 当前进行事务的节点
     */
    protected String              transactionalNodeName = null;

    private IConnectionHolder     ch;

    public StrictlTransaction(ExecutionContext ec){
        super(ec);

        this.ch = new StrictConnectionHolder();
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
            IConnection conn = null;
            if (transactionalNodeName != null) {// 已经有事务链接了
                if (transactionalNodeName.equalsIgnoreCase(groupName)
                    || IDataNodeExecutor.USE_LAST_DATA_NODE.equals(groupName)) {
                    conn = this.getConnectionHolder().getConnection(transactionalNodeName, ds);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_ACCROSS_DB_TRANSACTION,
                        transactionalNodeName,
                        groupName);

                }

                conn.setAutoCommit(false);
            } else {// 没有事务建立，新建事务
                transactionalNodeName = groupName;
                conn = getConnection(groupName, ds, rw);

            }
            return conn;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit() throws TddlException {
        if (logger.isDebugEnabled()) {
            logger.debug("commit");
        }
        lock.lock();

        try {
            Set<IConnection> conns = this.getConnectionHolder().getAllConnection();

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

        if (logger.isDebugEnabled()) {
            logger.debug("rollback");
        }
        lock.lock();

        try {

            Set<IConnection> conns = this.getConnectionHolder().getAllConnection();

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
        return ch;
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        lock.lock();

        try {
            this.getConnectionHolder().tryClose(conn, groupName);
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

            this.getConnectionHolder().closeAllConnections();

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void tryClose() throws SQLException {

    }

}
