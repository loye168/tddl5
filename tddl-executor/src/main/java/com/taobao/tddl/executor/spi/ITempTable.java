package com.taobao.tddl.executor.spi;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;

/**
 * 临时表
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午4:00:11
 * @since 5.0.0
 */
public interface ITempTable extends ITable {

    /**
     * 临时表返回给前段的meta
     */
    ICursorMeta returnMeta = null;

    public ICursorMeta getCursorMeta();

    public void setCursorMeta(ICursorMeta cursorMeta);

    public ISchematicCursor getCursor(ExecutionContext tmpContext) throws TddlException;
}
