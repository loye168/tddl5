package com.taobao.tddl.optimizer.core.plan.query;

import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

/**
 * 兼容以前corona的获取sequnece逻辑
 * 
 * @author jianghang 2014-5-19 下午3:55:03
 * @since 5.1.0
 */
public interface IGetSequence extends IDataNodeExecutor<IGetSequence> {

    public String getName();

    public void setName(String name);

    public int getCount();

    public void setCount(int count);
}
