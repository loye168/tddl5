package com.taobao.tddl.repo.hbase.cursor;

/**
 * hbase中表示最大值，转成byte的时候0ff来表示
 * @author mengshi.sunmengshi 2013-9-25 上午10:34:07
 * @since 3.0.1
 */
public class MaxValue implements ExtremeValue{
    
    @Override
    public byte getByte()
    {
        return -1;
    }
}
