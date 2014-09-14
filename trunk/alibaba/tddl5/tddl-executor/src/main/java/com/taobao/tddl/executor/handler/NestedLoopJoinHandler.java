package com.taobao.tddl.executor.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;

public class NestedLoopJoinHandler extends QueryHandlerCommon {

    public NestedLoopJoinHandler(){
        super();
    }

    @Override
    protected ISchematicCursor doQuery(ISchematicCursor cursor, IDataNodeExecutor executor,
                                       ExecutionContext executionContext) throws TddlException {

        IJoin join = (IJoin) executor;
        IRepository repo = executionContext.getCurrentRepository();
        IQueryTree leftQuery = join.getLeftNode();

        ISchematicCursor cursor_left = ExecutorContext.getContext()
            .getTopologyExecutor()
            .execByExecPlanNode(leftQuery, executionContext);

        if (!join.getRightNode().isSubQuery()) {
            /**
             * 右边调的是mget，所以不需要在创建的时候初始化
             */
            join.getRightNode().setLazyLoad(true);
        }
        ISchematicCursor cursor_right = ExecutorContext.getContext()
            .getTopologyExecutor()
            .execByExecPlanNode(join.getRightNode(), executionContext);

        if (join.getRightNode().isSubQuery()) {
            cursor_right = repo.getCursorFactory().tempTableCursor(executionContext,
                cursor_right,
                null,
                true,
                join.getRightNode().getRequestId());
        }
        cursor = repo.getCursorFactory().blockNestedLoopJoinCursor(executionContext,
            cursor_left,
            cursor_right,
            join.getLeftJoinOnColumns(),
            join.getRightJoinOnColumns(),
            join.getColumns(),
            join);
        return cursor;
    }

}
