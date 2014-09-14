package com.taobao.tddl.executor.transaction;

import java.sql.SQLException;

import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.IDataSource;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class AutoCommitConnectionHolder extends BaseConnectionHolder {

    private final static Logger logger = LoggerFactory.getLogger(AutoCommitConnectionHolder.class);

    public AutoCommitConnectionHolder(){

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

        IConnection conn = ds.getConnection();
        this.connections.add(conn);

        return conn;

    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {

        conn.close();

        this.connections.remove(conn);

        if (logger.isDebugEnabled()) {
            logger.debug("tryClose:" + conn);
        }
    }

}
