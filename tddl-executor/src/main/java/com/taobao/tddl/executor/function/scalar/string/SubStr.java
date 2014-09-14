package com.taobao.tddl.executor.function.scalar.string;

/**
 * <pre>
 * SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len)
 * 
 * SUBSTR() is a synonym for SUBSTRING().
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午2:00:32
 * @since 5.1.0
 */
public class SubStr extends SubString {

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SUBSTR" };
    }

}
