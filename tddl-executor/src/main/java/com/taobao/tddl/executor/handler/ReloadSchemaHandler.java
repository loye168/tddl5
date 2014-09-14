package com.taobao.tddl.executor.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.IAffectRowCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public class ReloadSchemaHandler extends HandlerCommon {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {

        int size = OptimizerContext.getContext().getSchemaManager().getAllTables().size();

        OptimizerContext.getContext().getSchemaManager().reload();

        IAffectRowCursor c = executionContext.getCurrentRepository()
            .getCursorFactory()
            .affectRowCursor(executionContext, size);

        return c;
    }
}
