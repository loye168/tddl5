package com.taobao.tddl.optimizer.core.plan.query;

public interface IShowWithTable extends IShow {

    public void setTableName(String tableName);

    public String getTableName();

    public String getActualTableName();

    public void setActualTableName(String actualTableName);
}
