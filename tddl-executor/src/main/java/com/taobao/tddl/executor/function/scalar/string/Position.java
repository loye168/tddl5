package com.taobao.tddl.executor.function.scalar.string;

/**
 * <pre>
 * POSITION(substr IN str)
 * 
 * POSITION(substr IN str) is a synonym for LOCATE(substr,str).
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月15日 下午2:37:28
 * @since 5.1.0
 */
public class Position extends Locate {

    @Override
    public String[] getFunctionNames() {

        return new String[] { "POSITION" };
    }

}
