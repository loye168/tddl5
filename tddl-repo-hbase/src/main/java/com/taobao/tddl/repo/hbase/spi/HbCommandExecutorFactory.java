package com.taobao.tddl.repo.hbase.spi;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.handler.ExplainHandler;
import com.taobao.tddl.executor.handler.IndexNestedLoopJoinHandler;
import com.taobao.tddl.executor.handler.MergeHandler;
import com.taobao.tddl.executor.handler.NestedLoopJoinHandler;
import com.taobao.tddl.executor.handler.SortMergeJoinHandler;
import com.taobao.tddl.executor.spi.ICommandHandler;
import com.taobao.tddl.executor.spi.ICommandHandlerFactory;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IPut.PUT_TYPE;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.hbase.handler.HbDeleteHandler;
import com.taobao.tddl.repo.hbase.handler.HbInsertHandler;
import com.taobao.tddl.repo.hbase.handler.HbQueryHandler;
import com.taobao.tddl.repo.hbase.handler.HbReplaceHandler;
import com.taobao.tddl.repo.hbase.handler.HbUpdateHandler;

/**
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2012-8-10 01:53:19
 */
public class HbCommandExecutorFactory implements ICommandHandlerFactory {

    private final ICommandHandler INSERT_HANDLER;
    private final ICommandHandler UPDATE_HANDLER;
    private final ICommandHandler DELETE_HANDLER;
    private final ICommandHandler REPLACE_HANDLER;
    private final ICommandHandler QUERY_HANDLER;
    private final ICommandHandler MERGE_HANDLER;
    private final ICommandHandler INDEX_NEST_LOOP_JOIN_HANDLER;
    private final ICommandHandler NEST_LOOP_JOIN_HANDLER;
    private final ICommandHandler SORT_MERGE_JOIN_HANDLER;
    private final ICommandHandler EXPLAIN_HANDLER;

    public HbCommandExecutorFactory(){
        INSERT_HANDLER = new HbInsertHandler();
        UPDATE_HANDLER = new HbUpdateHandler();
        DELETE_HANDLER = new HbDeleteHandler();
        REPLACE_HANDLER = new HbReplaceHandler();
        QUERY_HANDLER = new HbQueryHandler();
        MERGE_HANDLER = new MergeHandler();
        INDEX_NEST_LOOP_JOIN_HANDLER = new IndexNestedLoopJoinHandler();
        NEST_LOOP_JOIN_HANDLER = new NestedLoopJoinHandler();
        SORT_MERGE_JOIN_HANDLER = new SortMergeJoinHandler();
        EXPLAIN_HANDLER = new ExplainHandler();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ICommandHandler getCommandHandler(IDataNodeExecutor executor, ExecutionContext executionContext) {
        if (executor.isExplain()) {
            return EXPLAIN_HANDLER;
        }

        if (executor instanceof IQuery) {
            return QUERY_HANDLER;
        } else if (executor instanceof IMerge) {
            return MERGE_HANDLER;
        } else if (executor instanceof IJoin) {

            IJoin join = (IJoin) executor;
            JoinStrategy joinStrategy = join.getJoinStrategy();
            switch (joinStrategy) {
                case INDEX_NEST_LOOP:
                    return INDEX_NEST_LOOP_JOIN_HANDLER;
                case NEST_LOOP_JOIN:
                    return NEST_LOOP_JOIN_HANDLER;

                case SORT_MERGE_JOIN:
                    return SORT_MERGE_JOIN_HANDLER;
                default:
                    throw new IllegalArgumentException("should not be here");
            }
        } else if (executor instanceof IPut) {
            IPut put = (IPut) executor;
            PUT_TYPE putType = put.getPutType();
            switch (putType) {
                case REPLACE:
                    return REPLACE_HANDLER;
                case INSERT:
                    return INSERT_HANDLER;
                case DELETE:
                    return DELETE_HANDLER;
                case UPDATE:
                    return UPDATE_HANDLER;
                default:
                    throw new IllegalArgumentException("should not be here");
            }
        } else {
            throw new IllegalArgumentException("should not be here");
        }

    }
}
