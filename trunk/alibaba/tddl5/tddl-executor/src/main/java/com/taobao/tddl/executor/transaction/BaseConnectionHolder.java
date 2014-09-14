package com.taobao.tddl.executor.transaction;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.executor.common.IConnectionHolder;
import com.taobao.tddl.executor.exception.ExecutorException;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public abstract class BaseConnectionHolder implements IConnectionHolder {

    private final static Logger logger      = LoggerFactory.getLogger(BaseConnectionHolder.class);

    protected Set<IConnection>  connections = Collections.synchronizedSet(new HashSet());

    protected boolean           closed      = false;

    @Override
    public Set<IConnection> getAllConnection() {
        return this.connections;
    }

    @Override
    public void kill() {
        Set<IConnection> conns = this.getAllConnection();
        for (IConnection conn : conns) {
            try {
                conn.kill();
            } catch (Exception e) {
                logger.error("connection close failed, connection is " + conn, e);
            }
        }

        this.closeAllConnections();
        this.closed = true;
    }

    @Override
    public void cancel() {
        Set<IConnection> conns = this.getAllConnection();
        for (IConnection conn : conns) {
            try {
                conn.cancelQuery();
            } catch (Exception e) {
                logger.error("connection close failed, connection is " + conn, e);
            }
        }

    }

    protected void checkClosed() {
        if (!this.closed) {
            return;
        }

        throw new ExecutorException("connection holder is closed, cannot do any operations");
    }

    /**
     * 查询cancle掉之后，下次新的sql还能执行
     */
    @Override
    public void restart() {
        this.closed = false;

    }

    /**
     * 无条件关闭所有连接
     * 
     * @throws SQLException
     */
    @Override
    public void closeAllConnections() {
        Set<IConnection> conns = this.getAllConnection();

        for (IConnection conn : conns) {
            try {
                conn.close();
            } catch (Exception e) {
                logger.error("connection close failed, connection is " + conn, e);
            }
        }
        connections.clear();
    }

}
