package com.taobao.tddl.statistics;

import java.text.MessageFormat;

public class ExecuteSQLOperation extends AbstractSQLOperation {

    String                            groupName;
    String                            sql;

    public final static MessageFormat message = new MessageFormat("Execute sql on {0}, sql is: {1}, params is: {2}");

    @Override
    public String getOperationString() {

        return message.format(new String[] { groupName, sql });

    }

    public ExecuteSQLOperation(String groupName, String sql){
        super();
        this.groupName = groupName;
        this.sql = sql;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    @Override
    public String getSqlOrResult() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    @Override
    public String getOperationType() {
        return "Query";
    }

}
