package com.taobao.tddl.executor.transaction;

import java.sql.SQLException;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.IDataSource;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 不允许做任何跨库事务
 * 
 * @author mengshi.sunmengshi 2014年5月21日 下午4:13:47
 * @since 5.1.0
 */
public class StrictConnectionHolder extends BaseConnectionHolder {

    private final static Logger logger  = LoggerFactory.getLogger(StrictConnectionHolder.class);

    private IConnection         trxConn = null;
    private boolean             inUse   = false;

    public StrictConnectionHolder(){

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
        checkClosed();

        if (trxConn == null) {
            trxConn = ds.getConnection();
            connections.add(trxConn);
        }

        if (inUse) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONCURRENT_TRANSACTION, groupName);
        }

        inUse = true;

        if (logger.isDebugEnabled()) {
            logger.debug("getConnection:" + trxConn);
        }
        return trxConn;

    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {

        if (conn != this.trxConn) {
            throw new IllegalAccessError("impossible");
        }

        inUse = false;

        if (logger.isDebugEnabled()) {
            logger.debug("tryClose:" + trxConn);
        }
    }

    /**
     * 无条件关闭所有连接
     * 
     * @throws SQLException
     */
    @Override
    public void closeAllConnections() {
        super.closeAllConnections();
        this.trxConn = null;
    }

    @Override
    public void cancel() {
        super.cancel();
        this.trxConn = null;
    }

}
