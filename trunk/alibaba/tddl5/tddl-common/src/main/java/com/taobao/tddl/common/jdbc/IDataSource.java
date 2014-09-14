package com.taobao.tddl.common.jdbc;

import java.sql.SQLException;

import javax.sql.DataSource;

public interface IDataSource extends DataSource {

    @Override
    IConnection getConnection() throws SQLException;
}
