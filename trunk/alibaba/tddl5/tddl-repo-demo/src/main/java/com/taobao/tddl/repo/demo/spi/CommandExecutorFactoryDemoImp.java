package com.taobao.tddl.repo.demo.spi;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.handler.DeleteHandler;
import com.taobao.tddl.executor.handler.ExplainHandler;
import com.taobao.tddl.executor.handler.GetSequenceHandler;
import com.taobao.tddl.executor.handler.IndexNestedLoopJoinHandler;
import com.taobao.tddl.executor.handler.InsertHandler;
import com.taobao.tddl.executor.handler.MergeHandler;
import com.taobao.tddl.executor.handler.NestedLoopJoinHandler;
import com.taobao.tddl.executor.handler.QueryHandler;
import com.taobao.tddl.executor.handler.ReloadSchemaHandler;
import com.taobao.tddl.executor.handler.ReplaceHandler;
import com.taobao.tddl.executor.handler.ShowBroadcastsHandler;
import com.taobao.tddl.executor.handler.ShowDatasourcesHandler;
import com.taobao.tddl.executor.handler.ShowPartitionsHandler;
import com.taobao.tddl.executor.handler.ShowRuleHandler;
import com.taobao.tddl.executor.handler.ShowTablesHandler;
import com.taobao.tddl.executor.handler.ShowTopologyHandler;
import com.taobao.tddl.executor.handler.ShowTraceHandler;
import com.taobao.tddl.executor.handler.SortMergeJoinHandler;
import com.taobao.tddl.executor.handler.UpdateHandler;
import com.taobao.tddl.executor.spi.ICommandHandler;
import com.taobao.tddl.executor.spi.ICommandHandlerFactory;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IPut.PUT_TYPE;
import com.taobao.tddl.optimizer.core.plan.query.IGetSequence;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.core.plan.query.IReload;
import com.taobao.tddl.optimizer.core.plan.query.IShow;

/**
 * @author mengshi.sunmengshi 2014年4月10日 下午5:16:43
 * @since 5.1.0
 */
public class CommandExecutorFactoryDemoImp implements ICommandHandlerFactory {

    public CommandExecutorFactoryDemoImp(){
        INSERT_HANDLER = new InsertHandler();
        UPDATE_HANDLER = new UpdateHandler();
        DELETE_HANDLER = new DeleteHandler();
        REPLACE_HANDLER = new ReplaceHandler();
        QUERY_HANDLER = new QueryHandler();
        MERGE_HANDLER = new MergeHandler();
        INDEX_NEST_LOOP_JOIN_HANDLER = new IndexNestedLoopJoinHandler();
        NEST_LOOP_JOIN_HANDLER = new NestedLoopJoinHandler();
        SORT_MERGE_JOIN_HANDLER = new SortMergeJoinHandler();
        SHOW_TOPOLOGY_HANDLER = new ShowTopologyHandler();
        SHOW_PARITIONS_HANDLER = new ShowPartitionsHandler();
        SHOW_TABLES_HANDLER = new ShowTablesHandler();
        SHOW_BROADCASTS_HANDLER = new ShowBroadcastsHandler();
        SHOW_TRACE_HANDLER = new ShowTraceHandler();
        SHOW_RULE_HANDLER = new ShowRuleHandler();
        GET_SEQUENCE_HANDLER = new GetSequenceHandler();
        RELOAD_SCHEMA_HANDLER = new ReloadSchemaHandler();
        EXPLAIN_HANDLER = new ExplainHandler();
        SHOW_DATASOURCES_HANDLER = new ShowDatasourcesHandler();
    }

    private final ICommandHandler INSERT_HANDLER;
    private final ICommandHandler UPDATE_HANDLER;
    private final ICommandHandler DELETE_HANDLER;
    private final ICommandHandler REPLACE_HANDLER;
    private final ICommandHandler QUERY_HANDLER;
    private final ICommandHandler MERGE_HANDLER;
    private final ICommandHandler INDEX_NEST_LOOP_JOIN_HANDLER;
    private final ICommandHandler NEST_LOOP_JOIN_HANDLER;
    private final ICommandHandler SORT_MERGE_JOIN_HANDLER;
    private final ICommandHandler SHOW_TOPOLOGY_HANDLER;
    private final ICommandHandler SHOW_BROADCASTS_HANDLER;
    private final ICommandHandler SHOW_PARITIONS_HANDLER;
    private final ICommandHandler SHOW_TRACE_HANDLER;
    private final ICommandHandler SHOW_TABLES_HANDLER;
    private final ICommandHandler SHOW_RULE_HANDLER;
    private final ICommandHandler GET_SEQUENCE_HANDLER;
    private final ICommandHandler RELOAD_SCHEMA_HANDLER;
    private final ICommandHandler EXPLAIN_HANDLER;
    private final ICommandHandler SHOW_DATASOURCES_HANDLER;

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
        } else if (executor instanceof IShow) {
            switch (((IShow) executor).getType()) {
                case TOPOLOGY:
                    return SHOW_TOPOLOGY_HANDLER;
                case BRAODCASTS:
                    return SHOW_BROADCASTS_HANDLER;
                case RULE:
                    return SHOW_RULE_HANDLER;
                case PARTITIONS:
                    return SHOW_PARITIONS_HANDLER;
                case TABLES:
                    return SHOW_TABLES_HANDLER;
                case TRACE:
                    return SHOW_TRACE_HANDLER;
                case DATASOURCES:
                    return SHOW_DATASOURCES_HANDLER;
                default:
                    throw new IllegalArgumentException("should not be here , type : " + ((IShow) executor).getType());

            }
        } else if (executor instanceof IReload) {

            switch (((IReload) executor).getType()) {
                case SCHEMA:
                    return RELOAD_SCHEMA_HANDLER;
                default:
                    throw new IllegalArgumentException("should not be here , type : " + ((IReload) executor).getType());

            }

        } else if (executor instanceof IGetSequence) {
            return GET_SEQUENCE_HANDLER;
        } else {
            throw new IllegalArgumentException("should not be here");
        }
    }
}
