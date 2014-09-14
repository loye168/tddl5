package com.taobao.tddl.repo.mysql.cursor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.cursor.IResultSetCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;

public class ResultSetCursor extends ResultCursor implements IResultSetCursor {

    private ResultSet      rs;
    private My_JdbcHandler jdbcHandler = null;

    public ResultSetCursor(ResultSet rs, My_JdbcHandler jdbcHandler){
        super(null);
        this.rs = rs;

        this.jdbcHandler = jdbcHandler;
    }

    @Override
    public ResultSet getResultSet() {
        return this.rs;
    }

    @Override
    public IRowSet current() throws TddlException {
        return jdbcHandler.getCurrent();
    }

    @Override
    public IRowSet next() throws TddlException {
        try {
            return jdbcHandler.next();
        } catch (SQLException e) {
            throw new TddlException(e);
        }
    }

    @Override
    public IRowSet prev() throws TddlException {
        try {
            return jdbcHandler.prev();
        } catch (SQLException e) {
            throw new TddlException(e);
        }
    }

    @Override
    public List<TddlException> close(List<TddlException> exs) {
        if (exs == null) {
            exs = new ArrayList();
        }
        try {
            rs.close();
        } catch (Exception e) {
            exs.add(new TddlException(e));
        }

        return exs;
    }

    @Override
    public IRowSet first() throws TddlException {
        try {
            return jdbcHandler.first();
        } catch (SQLException e) {
            throw new TddlException(e);
        }
    }

    @Override
    public IRowSet last() throws TddlException {
        try {
            return jdbcHandler.last();
        } catch (SQLException e) {
            throw new TddlException(e);
        }
    }
}
