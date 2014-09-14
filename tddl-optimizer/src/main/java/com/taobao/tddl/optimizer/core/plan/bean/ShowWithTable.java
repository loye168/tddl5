package com.taobao.tddl.optimizer.core.plan.bean;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;

import com.taobao.tddl.optimizer.core.plan.query.IShowWithTable;
import com.taobao.tddl.optimizer.utils.OptimizerToString;

public class ShowWithTable extends Show implements IShowWithTable {

    protected String tableName;
    protected String actualTableName;

    @Override
    public String toStringWithInden(int inden, ExplainMode mode) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        appendln(sb, tabTittle + getSql());
        appendField(sb, "where", this.getWhereFilter(), tabContent);
        appendField(sb, "executeOn", this.getDataNode(), tabContent);
        return sb.toString();
    }

    @Override
    public ShowWithTable copy() {
        ShowWithTable newShow = new ShowWithTable();
        newShow.setTableName(this.tableName);
        newShow.setType(this.type);
        newShow.setSql(this.sql);
        newShow.setFull(this.full);
        newShow.setActualTableName(this.actualTableName);
        newShow.executeOn(this.getDataNode());
        newShow.setWhereFilter(this.getWhereFilter());
        newShow.setPattern(this.pattern);
        return newShow;
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
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    public String getActualTableName() {
        return actualTableName;
    }

    public void setActualTableName(String actualTableName) {
        this.actualTableName = actualTableName;
    }

}
