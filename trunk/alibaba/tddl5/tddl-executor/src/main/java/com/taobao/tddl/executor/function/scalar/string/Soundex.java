package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * 不支持！太奇葩！
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午5:40:09
 * @since 5.1.0
 */
public class Soundex extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SOUNDEX" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        throw new UnsupportedOperationException("如果没法下推，soundex不支持");

    }

}
