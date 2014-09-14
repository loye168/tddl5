package com.taobao.tddl.repo.mysql.handler;

import com.taobao.tddl.executor.common.ExecutionContext;
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
import com.taobao.tddl.optimizer.core.plan.query.IShow;

/**
 * @author mengshi.sunmengshi 2013-12-5 下午6:18:33
 * @since 5.0.0
 */
public class CommandHandlerFactoryMyImp implements ICommandHandlerFactory {

    public CommandHandlerFactoryMyImp(){
        INSERT_HANDLER = new InsertMyHandler();
        UPDATE_HANDLER = new UpdateMyHandler();
        DELETE_HANDLER = new DeleteMyHandler();
        REPLACE_HANDLER = new ReplaceMyHandler();
        QUERY_HANDLER = new QueryMyHandler();
        MERGE_HANDLER = new MergeHandler();
        INDEX_NEST_LOOP_JOIN_HANDLER = new IndexNestedLoopJoinHandler();
        NEST_LOOP_JOIN_HANDLER = new NestedLoopJoinHandler();
        SORT_MERGE_JOIN_HANDLER = new SortMergeJoinHandler();
        CONDENSABLE_JOIN_HANDLER = new QueryMyHandler();
        SHOW_TABLES_HANDLER = new ShowTablesMyHandler();
        SHOW_DIRECT_HANDLER = new ShowDirectMyHandler();
        SHOW_CREATE_TABLE_HANDLER = new ShowCreateTableMyHandler();
        EXPLAIN_HANDLER = new ExplainMyHandler();
    }

    protected QueryMyHandler  CONDENSABLE_JOIN_HANDLER;
    protected ICommandHandler INSERT_HANDLER;
    protected ICommandHandler UPDATE_HANDLER;
    protected ICommandHandler DELETE_HANDLER;
    protected ICommandHandler REPLACE_HANDLER;
    protected ICommandHandler QUERY_HANDLER;
    protected ICommandHandler MERGE_HANDLER;
    protected ICommandHandler INDEX_NEST_LOOP_JOIN_HANDLER;
    protected ICommandHandler NEST_LOOP_JOIN_HANDLER;
    protected ICommandHandler SORT_MERGE_JOIN_HANDLER;
    protected ICommandHandler SHOW_TABLES_HANDLER;
    protected ICommandHandler SHOW_DIRECT_HANDLER;
    protected ICommandHandler SHOW_CREATE_TABLE_HANDLER;
    protected ICommandHandler EXPLAIN_HANDLER;

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
            boolean isCondensable = isCondensable(executor);
            if (isCondensable) {
                return CONDENSABLE_JOIN_HANDLER;
            }

            IJoin join = (IJoin) executor;
            JoinStrategy joinStrategy = join.getJoinStrategy();
            switch (joinStrategy) {
                case INDEX_NEST_LOOP:
                    return INDEX_NEST_LOOP_JOIN_HANDLER;
                case NEST_LOOP_JOIN:
                    if (join.getRightNode().isSubQuery()) {
                        return NEST_LOOP_JOIN_HANDLER;
                    } else {
                        // mysql支持非主键的查询，所以即使是nest_loop，也可以选择index的模式
                        return INDEX_NEST_LOOP_JOIN_HANDLER;
                    }
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
        } else if (executor instanceof IShow) {
            switch (((IShow) executor).getType()) {
                case TABLES:
                    return SHOW_TABLES_HANDLER;
                case CREATE_TABLE:
                    return SHOW_CREATE_TABLE_HANDLER;
                case DESC:
                case INDEX:
                case INDEXES:
                case KEYS:
                case COLUMNS:
                    return SHOW_DIRECT_HANDLER;
                default:
                    throw new IllegalArgumentException("should not be here");
            }
        } else {
            throw new IllegalArgumentException("should not be here");
        }
    }

    private boolean isCondensable(IDataNodeExecutor executor) {
        IJoin ijoin = (IJoin) executor;
        String leftNode = ijoin.getLeftNode().getDataNode();
        String rightNode = ijoin.getRightNode().getDataNode();
        if (leftNode == null || rightNode == null) {
            return false;
        } else if (!leftNode.equals(rightNode)) {
            return false;
        }

        if (ijoin.getLeftNode() instanceof IMerge || ijoin.getRightNode() instanceof IMerge) {
            return false;
        }
        boolean leftJoin = true;
        boolean rightJoin = true;
        if (ijoin.getLeftNode() instanceof IJoin) {
            leftJoin = isCondensable(ijoin.getLeftNode());
        }
        if (ijoin.getRightNode() instanceof IJoin) {
            rightJoin = isCondensable(ijoin.getRightNode());
        }

        return leftJoin & rightJoin;
    }
}
