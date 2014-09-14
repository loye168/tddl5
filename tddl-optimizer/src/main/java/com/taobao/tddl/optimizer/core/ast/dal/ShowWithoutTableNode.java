package com.taobao.tddl.optimizer.core.ast.dal;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.bean.ShowWithoutTable;

public class ShowWithoutTableNode extends BaseShowNode {

    public ShowWithoutTableNode(){
        super();
    }

    @Override
    public void build() {
        super.build();
    }

    @Override
    public IDataNodeExecutor toDataNodeExecutor(int shareIndex) {
        ShowWithoutTable show = new ShowWithoutTable();
        show.setType(type);
        show.executeOn(this.getDataNode());
        show.setWhereFilter(this.getWhereFilter());
        show.setPattern(this.pattern);
        show.setFull(this.full);
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
        return "SHOW " + (full ? "FULL " : "") + this.type.name() + (pattern != null ? " LIKE " + pattern : "");
    }

    @Override
    public IFunction getNextSubqueryOnFilter() {
        return null;
    }

    @Override
    public ShowWithoutTableNode copy() {
        return deepCopy();
    }

    @Override
    public ShowWithoutTableNode copySelf() {
        return deepCopy();
    }

    @Override
    public ShowWithoutTableNode deepCopy() {
        ShowWithoutTableNode node = new ShowWithoutTableNode();
        node.setType(this.type);
        node.executeOn(this.getDataNode());
        node.setWhereFilter(this.getWhereFilter());
        node.setPattern(this.pattern);
        node.setFull(this.full);
        return node;
    }

}
