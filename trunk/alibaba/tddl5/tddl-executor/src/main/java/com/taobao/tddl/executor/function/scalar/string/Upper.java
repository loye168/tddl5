package com.taobao.tddl.executor.function.scalar.string;

/**
 * same as ucase
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午5:16:01
 * @since 5.1.0
 */
public class Upper extends Ucase {

    @Override
    public String[] getFunctionNames() {
        return new String[] { "UPPER" };
    }

}
