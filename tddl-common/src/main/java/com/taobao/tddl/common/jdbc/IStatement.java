package com.taobao.tddl.common.jdbc;

import java.io.InputStream;
import java.sql.Statement;

/**
 * @author mengshi.sunmengshi 2014年5月15日 上午11:25:22
 * @since 5.1.0
 */
public interface IStatement extends Statement {

    /**
     * Sets an InputStream instance that will be used to send data to the MySQL
     * server for a "LOAD DATA LOCAL INFILE" statement rather than a
     * FileInputStream or URLInputStream that represents the path given as an
     * argument to the statement. This stream will be read to completion upon
     * execution of a "LOAD DATA LOCAL INFILE" statement, and will automatically
     * be closed by the driver, so it needs to be reset before each call to
     * execute*() that would cause the MySQL server to request data to fulfill
     * the request for "LOAD DATA LOCAL INFILE". If this value is set to NULL,
     * the driver will revert to using a FileInputStream or URLInputStream as
     * required.
     */
    public abstract void setLocalInfileInputStream(InputStream stream);

    /**
     * Returns the InputStream instance that will be used to send data in
     * response to a "LOAD DATA LOCAL INFILE" statement. This method returns
     * NULL if no such stream has been set via setLocalInfileInputStream().
     */
    public abstract InputStream getLocalInfileInputStream();

    public String getAppName();
}
