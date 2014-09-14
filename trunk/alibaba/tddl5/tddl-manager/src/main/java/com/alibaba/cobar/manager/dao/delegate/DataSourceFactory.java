package com.alibaba.cobar.manager.dao.delegate;

import javax.sql.DataSource;

public interface DataSourceFactory {

    public DataSource createDataSource(String ip, int port, String user, String password);
}
