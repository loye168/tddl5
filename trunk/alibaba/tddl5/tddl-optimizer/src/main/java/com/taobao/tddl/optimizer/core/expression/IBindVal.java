package com.taobao.tddl.optimizer.core.expression;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.CanVisit;
import com.taobao.tddl.optimizer.core.PlanVisitor;

/**
 * 绑定变量
 */
public interface IBindVal extends Comparable, CanVisit {

    public Object assignment(Parameters parameterSettings);

    @Override
    void accept(PlanVisitor visitor);

    /**
     * 返回用户设定绑定变量的原始下标，真正执行时下表会变化
     */
    Integer getOrignIndex();

    /**
     * 返回绑定变量的具体值
     */
    Object getValue();

    IBindVal copy();
}
