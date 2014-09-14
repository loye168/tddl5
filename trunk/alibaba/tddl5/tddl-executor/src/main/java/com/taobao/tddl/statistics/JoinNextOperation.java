package com.taobao.tddl.statistics;

import java.text.MessageFormat;

import com.taobao.tddl.executor.rowset.IRowSet;

public class JoinNextOperation extends AbstractSQLOperation {

    public IRowSet                    row;
    public final static MessageFormat message = new MessageFormat("Join a row: {0}");

    public JoinNextOperation(IRowSet row){
        super();
        this.row = row;
    }

    @Override
    public String getOperationString() {
        return message.format(new String[] { row.toString() });
    }

    @Override
    public String getOperationType() {
        return "Join";
    }

    @Override
    public String getSqlOrResult() {
        return null;
    }

}
