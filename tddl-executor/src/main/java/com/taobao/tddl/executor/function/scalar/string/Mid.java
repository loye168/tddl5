package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.executor.common.ExecutionContext;

/**
 * <pre>
 * MID(str,pos,len)
 * 
 * MID(str,pos,len) is a synonym for SUBSTRING(str,pos,len).
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午2:34:55
 * @since 5.1.0
 */
public class Mid extends SubString {

    @Override
    public String[] getFunctionNames() {
        return new String[] { "MID" };
    }

    @SuppressWarnings("unused")
    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object a = args[2];
        return super.compute(args, ec);
    }
}
