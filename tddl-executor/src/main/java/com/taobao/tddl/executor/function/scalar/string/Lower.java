package com.taobao.tddl.executor.function.scalar.string;

/**
 * same as lcase
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午5:14:54
 * @since 5.1.0
 */
public class Lower extends Lcase {

    @Override
    public String[] getFunctionNames() {
        return new String[] { "LOWER" };
    }

}
