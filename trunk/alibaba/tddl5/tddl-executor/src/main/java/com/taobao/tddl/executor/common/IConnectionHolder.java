package com.taobao.tddl.executor.common;

import java.sql.SQLException;
import java.util.Set;

import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.IDataSource;

public interface IConnectionHolder {

    public IConnection getConnection(String groupName, IDataSource ds) throws SQLException;

    public void closeAllConnections();

    public void tryClose(IConnection conn, String groupName) throws SQLException;

    public Set<IConnection> getAllConnection();

    public void kill();

    public void cancel();

    public void restart();
}
