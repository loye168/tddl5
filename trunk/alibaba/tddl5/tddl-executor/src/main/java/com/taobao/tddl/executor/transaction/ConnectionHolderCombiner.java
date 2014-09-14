package com.taobao.tddl.executor.transaction;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.IDataSource;
import com.taobao.tddl.executor.common.IConnectionHolder;

public class ConnectionHolderCombiner extends BaseConnectionHolder {

    private IConnectionHolder ch2;
    private IConnectionHolder ch1;

    ConnectionHolderCombiner(IConnectionHolder ch1, IConnectionHolder ch2){
        this.ch1 = ch1;
        this.ch2 = ch2;
    }

    @Override
    public Set<IConnection> getAllConnection() {

        Set<IConnection> conns = new HashSet<IConnection>();
        conns.addAll(ch1.getAllConnection());
        conns.addAll(ch2.getAllConnection());

        return conns;
    }

    @Override
    public IConnection getConnection(String groupName, IDataSource ds) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void tryClose(IConnection conn, String groupName) throws SQLException {
        throw new UnsupportedOperationException();

    }

    @Override
    public void kill() {
        ch1.kill();
        ch2.kill();
    }

    @Override
    public void cancel() {
        ch1.cancel();
        ch2.cancel();
    }

}
