package com.taobao.tddl.optimizer.core.plan.bean;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;

import com.taobao.tddl.optimizer.core.plan.query.IShowWithoutTable;
import com.taobao.tddl.optimizer.utils.OptimizerToString;

public class ShowWithoutTable extends Show implements IShowWithoutTable {

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
    public ShowWithoutTable copy() {
        ShowWithoutTable newShow = new ShowWithoutTable();
        newShow.setType(this.type);
        newShow.setSql(this.sql);
        newShow.executeOn(this.getDataNode());
        newShow.setWhereFilter(this.getWhereFilter());
        newShow.setPattern(this.pattern);
        newShow.setFull(full);
        return newShow;
    }

    public String getSql() {
        return "SHOW " + (full ? "FULL " : "") + this.type.name() + (pattern != null ? " LIKE " + pattern : "");
    }

}
