package com.taobao.tddl.repo.hbase.cursor;

/**
 * hbase中表示最小值，转成byte的时候0x00来表示
 * @author mengshi.sunmengshi 2013-9-25 上午10:34:07
 * @since 3.0.1
 */
public class MinValue implements ExtremeValue{
    
    @Override
    public byte getByte()
    {
        return 0;
    }
}
