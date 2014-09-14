package com.taobao.tddl.executor.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IShow;

public abstract class BaseShowHandler extends HandlerCommon {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        IShow show = (IShow) executor;
        ISchematicCursor showCursor = doShow(show, executionContext);
        if (show.getWhereFilter() != null) {
            showCursor = executionContext.getCurrentRepository()
                .getCursorFactory()
                .valueFilterCursor(executionContext, showCursor, show.getWhereFilter());

        }

        return showCursor;
    }

    public abstract ISchematicCursor doShow(IShow show, ExecutionContext executionContext) throws TddlException;

}
