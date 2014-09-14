package com.taobao.tddl.common.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.SqlMetaData;

public interface IConnection extends Connection {

    public void kill() throws SQLException;

    public void cancelQuery() throws SQLException;

    public long getLastInsertId();

    public void setLastInsertId(long id);

    public List<Long> getGeneratedKeys();

    public void setGeneratedKeys(List<Long> ids);

    public ITransactionPolicy getTrxPolicy();

    public void setTrxPolicy(ITransactionPolicy trxPolicy) throws TddlException;

    public void setEncoding(String encoding);

    public String getEncoding();

    public void setSqlMode(String sqlMode);

    public String getSqlMode();

    /**
     * 传递该sql的元信息给底层
     * 
     * @param sqlMetaData
     */
    public void setMetaData(SqlMetaData sqlMetaData);

    /**
     * 获取sql meta data
     */
    public SqlMetaData getSqlMetaData();
}
