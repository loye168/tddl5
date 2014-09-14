package com.taobao.tddl.executor.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.ICommandHandler;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public class SubQueryHandler implements ICommandHandler {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

}
