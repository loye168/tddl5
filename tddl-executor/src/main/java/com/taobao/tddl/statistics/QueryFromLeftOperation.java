package com.taobao.tddl.statistics;

import java.text.MessageFormat;

import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public class QueryFromLeftOperation extends AbstractSQLOperation {

    public final static MessageFormat message = new MessageFormat("Get from left: {0}={1}");
    private IDataNodeExecutor         query;
    private String                    sql;
    private String                    left;

    public QueryFromLeftOperation(String left){
        super();

        this.left = left;

    }

    @Override
    public String getOperationString() {

        return null;
    }

    @Override
    public String getOperationType() {
        return "Query From Left";
    }

    @Override
    public String getSqlOrResult() {
        return "SELECT * FROM LEFT " + left;
    }

}
