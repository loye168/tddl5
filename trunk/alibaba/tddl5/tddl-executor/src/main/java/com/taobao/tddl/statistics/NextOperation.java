package com.taobao.tddl.statistics;

import java.text.MessageFormat;

import com.taobao.tddl.executor.rowset.IRowSet;

public class NextOperation extends AbstractSQLOperation {

    public IRowSet                    row;
    public final static MessageFormat message = new MessageFormat("Get a row: {0}");

    public NextOperation(IRowSet row){
        super();
        this.row = row;
    }

    @Override
    public String getOperationString() {
        return message.format(new String[] { row.toString() });
    }

    @Override
    public String getOperationType() {
        return "Fetch";
    }

    @Override
    public String getSqlOrResult() {
        return null;
    }

}
