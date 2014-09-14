package com.taobao.tddl.optimizer.core.plan.bean;

import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.ast.dal.BaseShowNode.ShowType;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.plan.query.IShow;

public abstract class Show extends DataNodeExecutor<IShow> implements IShow {

    protected ShowType type;
    protected IFilter  filter;
    protected String   pattern;
    protected boolean  full;

    @Override
    public ShowType getType() {
        return type;
    }

    @Override
    public void setType(ShowType type) {
        this.type = type;
    }

    @Override
    public void accept(PlanVisitor visitor) {
    }

    @Override
    public void setWhereFilter(IFilter filter) {
        this.filter = filter;
    }

    @Override
    public IFilter getWhereFilter() {
        return this.filter;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

}
