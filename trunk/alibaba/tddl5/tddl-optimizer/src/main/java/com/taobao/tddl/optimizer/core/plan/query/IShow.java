package com.taobao.tddl.optimizer.core.plan.query;

import com.taobao.tddl.optimizer.core.ast.dal.BaseShowNode.ShowType;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public interface IShow extends IDataNodeExecutor<IShow> {

    public ShowType getType();

    public void setType(ShowType type);

    public void setWhereFilter(IFilter filter);

    public IFilter getWhereFilter();

    public String getPattern();

    public void setPattern(String pattern);

    public boolean isFull();

    public void setFull(boolean full);
}
