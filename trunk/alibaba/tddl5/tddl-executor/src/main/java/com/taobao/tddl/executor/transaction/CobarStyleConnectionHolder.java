package com.taobao.tddl.executor.transaction;

import java.sql.SQLException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.IDataSource;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class CobarStyleConnectionHolder extends BaseConnectionHolder {

    private final static Logger             logger   = LoggerFactory.getLogger(CobarStyleConnectionHolder.class);

    private Map<String, Queue<IConnection>> connsMap = new ConcurrentHashMap<String, Queue<IConnection>>();

    public CobarStyleConnectionHolder(){

    }

    /**
     * @param groupName
     * @param ds
     * @param reuse 是否重用已有的
     * @return
     * @throws SQLException
     */
    @Override
    public IConnection getConnection(String groupName, IDataSource ds) throws SQLException {
        Queue<IConnection> conns = this.connsMap.get(groupName);

        if (conns == null) {
            conns = new LinkedBlockingQueue<IConnection>();
            this.connsMap.put(groupName, conns);
        }

        if (!conns.isEmpty()) {
            return conns.poll();
        }

        IConnection conn = ds.getConnection();
        this.connections.add(conn);

        return conn;

    }

    @Override
    public void tryClose(IConnection conn, String groupName) {
        Queue<IConnection> conns = this.connsMap.get(groupName);

        if (conns == null) {
            throw new IllegalAccessError("impossible");
        }

        conns.offer(conn);

        if (logger.isDebugEnabled()) {
            logger.debug("tryClose:" + conn);
        }
    }

    @Override
    public void closeAllConnections() {
        super.closeAllConnections();
        this.connsMap.clear();
    }

    @Override
    public void cancel() {
        super.cancel();
        this.connsMap.clear();
    }
}
