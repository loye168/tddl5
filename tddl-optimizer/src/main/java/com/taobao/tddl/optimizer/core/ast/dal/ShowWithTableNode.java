package com.taobao.tddl.optimizer.core.ast.dal;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.bean.ShowWithTable;

public class ShowWithTableNode extends BaseShowNode {

    protected String tableName;
    protected String actualTableName;

    public ShowWithTableNode(String tableName){
        super();
        this.tableName = tableName;
    }

    @Override
    public void build() {
        if (tableName == null) {
            throw new IllegalArgumentException("tableName is null");
        }

        OptimizerContext.getContext().getSchemaManager().getTable(tableName);
        super.build();
    }

    @Override
    public IDataNodeExecutor toDataNodeExecutor(int shareIndex) {
        ShowWithTable show = new ShowWithTable();
        show.setType(type);
        show.setFull(full);
        show.setTableName(this.tableName);
        show.setActualTableName(this.actualTableName);
        show.executeOn(this.getDataNode());
        show.setWhereFilter(this.getWhereFilter());
        show.setPattern(this.pattern);
        return show;
    }

    @Override
    public void assignment(Parameters parameterSettings) {

    }

    @Override
    public boolean isNeedBuild() {
        return false;
    }

    @Override
    public String toString(int inden, int shareIndex) {
        return getSql();
    }

    public String getSql() {
        String tableName = this.tableName;
        if (this.actualTableName != null) {
            tableName = this.actualTableName;
        }

        switch (type) {
            case DESC:
                return "DESC " + tableName;
            case CREATE_TABLE:
                return "SHOW CREATE TABLE " + tableName;
            default:
                return "SHOW " + (full ? "FULL " : "") + this.type.name() + " FROM " + tableName
                       + (pattern != null ? " LIKE " + pattern : "");
        }
    }

    @Override
    public IFunction getNextSubqueryOnFilter() {
        return null;
    }

    @Override
    public ShowWithTableNode copy() {
        return deepCopy();
    }

    @Override
    public ShowWithTableNode copySelf() {
        return deepCopy();
    }

    @Override
    public ShowWithTableNode deepCopy() {
        ShowWithTableNode node = new ShowWithTableNode(tableName);
        node.setType(this.type);
        node.setFull(this.full);
        node.setPattern(this.pattern);
        node.executeOn(this.getDataNode());
        node.setWhereFilter(this.getWhereFilter());
        node.setActualTableName(actualTableName);
        return node;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getActualTableName() {
        return actualTableName;
    }

    public void setActualTableName(String actualTableName) {
        this.actualTableName = actualTableName;
    }

}
