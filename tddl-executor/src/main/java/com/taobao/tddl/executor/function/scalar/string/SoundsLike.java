package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.scalar.filter.Filter;

/**
 * 不支持
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午5:44:59
 * @since 5.1.0
 */
public class SoundsLike extends Filter {

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SOUNDS LIKE" };
    }

    @Override
    protected Object computeInner(Object[] args, ExecutionContext ec) {

        throw new UnsupportedOperationException("如果没法下推，sounds like不支持");

    }

}
