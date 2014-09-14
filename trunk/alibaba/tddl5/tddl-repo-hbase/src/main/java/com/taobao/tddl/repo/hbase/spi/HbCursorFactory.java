package com.taobao.tddl.repo.hbase.spi;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.IRangeCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.CursorFactoryDefaultImpl;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.repo.hbase.cursor.HbRangeCursor;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 */
public class HbCursorFactory extends CursorFactoryDefaultImpl {

    public HbCursorFactory(){
    }

    @Override
    public IRangeCursor rangeCursor(ExecutionContext executionContext, ISchematicCursor cursor, IFilter lf)
                                                                                                           throws TddlException {
        return new HbRangeCursor(cursor, lf, executionContext);
    }
}
