package com.taobao.tddl.executor.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.IAffectRowCursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.IMergeCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ConcurrentMergeCursor;
import com.taobao.tddl.executor.cursor.impl.DistinctCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.IQueryTree.QUERY_CONCURRENCY;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

public class MergeHandler extends QueryHandlerCommon {

    public MergeHandler(){
        super();
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected ISchematicCursor doQuery(ISchematicCursor cursor, IDataNodeExecutor executor,
                                       ExecutionContext executionContext) throws TddlException {
        // merge，多个相同schema cursor的合并操作。
        IMerge merge = (IMerge) executor;
        IRepository repo = executionContext.getCurrentRepository();
        List<IDataNodeExecutor> subNodes = merge.getSubNodes();
        List<ISchematicCursor> subCursors = new ArrayList<ISchematicCursor>();

        if (!merge.isSharded()) {
            /*
             * 如果是个需要左驱动表的结果来进行查询的查询，直接返回mergeCursor.
             * 有些查询，是需要依赖左值结果进行查询的。这类查询需要先取一批左值出来，根据这些左值，走规则算右值的。
             * 这时候只有一个subNodes
             */
            if (subNodes.size() != 1) {
                throw new IllegalArgumentException("subNodes is not 1? may be 执行计划生育上有了问题了，查一下" + executor);
            }
            TableAndIndex ti = new TableAndIndex();

            IQuery ide = (IQuery) subNodes.get(0);
            buildTableAndMetaLogicalIndex(ide, ti, executionContext);

            ICursorMeta iCursorMetaTemp = ExecUtils.convertToICursorMeta(ide);

            ColumnMeta[] keyColumns = new ColumnMeta[] {};

            List<IOrderBy> tempOrderBy = new LinkedList<IOrderBy>();
            for (ColumnMeta cm : keyColumns) {
                IOrderBy ob = ASTNodeFactory.getInstance().createOrderBy();
                ob.setColumn(ExecUtils.getIColumnsFromColumnMeta(cm, ide.getAlias()));
                tempOrderBy.add(ob);
            }
            cursor = repo.getCursorFactory().mergeCursor(executionContext,
                subCursors,
                iCursorMetaTemp,
                merge,
                tempOrderBy);
            return cursor;
        } else {

            if (merge.isDmlByBroadcast()) {
                executeBroadCast(executionContext, subNodes, subCursors);
            } else if (QUERY_CONCURRENCY.CONCURRENT == merge.getQueryConcurrency() && executionContext.isAutoCommit()) {
                executeSubNodesFuture(executionContext, subNodes, subCursors);
            } else if (QUERY_CONCURRENCY.GROUP_CONCURRENT == merge.getQueryConcurrency()
                       && executionContext.isAutoCommit()) {
                executeGroupConcurrent(executionContext, subNodes, subCursors);
            } else {
                executeSubNodes(executionContext, subNodes, subCursors);

            }
        }
        if (subNodes.get(0) instanceof IPut) {// 合并affect_rows
            int affect_rows = 0;
            boolean first = true;
            List<TddlException> exs = new ArrayList();
            for (ISchematicCursor affectRowCursor : subCursors) {
                IRowSet rowSet = null;

                if (!merge.isDistinctByShardColumns() || first) {
                    while ((rowSet = affectRowCursor.next()) != null) {
                        Integer affectRow = rowSet.getInteger(0);
                        if (affectRow != null) {
                            affect_rows += affectRow;
                        }
                    }
                }

                first = false;

                exs = affectRowCursor.close(exs);

            }
            if (!exs.isEmpty()) throw (GeneralUtil.mergeException(exs));
            IAffectRowCursor affectRow = repo.getCursorFactory().affectRowCursor(executionContext, affect_rows);
            return affectRow;
        } else {

            // union的话要去重
            // 这里假设都是排好序的
            if (merge.isUnion()) {
                cursor = this.buildMergeSortCursor(executionContext, repo, subCursors, false);
            } else {
                cursor = repo.getCursorFactory().mergeCursor(executionContext, subCursors, merge);
            }
        }
        return cursor;
    }

    private void executeBroadCast(ExecutionContext executionContext, List<IDataNodeExecutor> subNodes,
                                  List<ISchematicCursor> subCursors) throws TddlException {

        // 第一个节点当成主库
        // 主库成功则成功，其余的库失败不报错
        IDataNodeExecutor firstNode = subNodes.get(0);

        ISchematicCursor rc = ExecutorContext.getContext()
            .getTopologyExecutor()
            .execByExecPlanNode(firstNode, executionContext);

        subCursors.add(rc);

        if (subNodes.size() > 1) {
            List<IDataNodeExecutor> otherNodes = new ArrayList(subNodes.size() - 1);
            for (int i = 1; i < subNodes.size(); i++) {
                otherNodes.add(subNodes.get(i));
            }
            try {
                executeSubNodesFuture(executionContext, otherNodes, subCursors);
            } catch (Exception ex) {
                logger.warn("broadcast has failed on some group, sql is: " + executionContext.getSql(), ex);
            }
        }

    }

    private void executeSubNodes(ExecutionContext executionContext, List<IDataNodeExecutor> subNodes,
                                 List<ISchematicCursor> subCursors) throws TddlException {
        for (IDataNodeExecutor q : subNodes) {
            ISchematicCursor rc = ExecutorContext.getContext()
                .getTopologyExecutor()
                .execByExecPlanNode(q, executionContext);
            subCursors.add(rc);
        }
    }

    @SuppressWarnings("rawtypes")
    private void executeSubNodesFuture(ExecutionContext executionContext, List<IDataNodeExecutor> subNodes,
                                       List<ISchematicCursor> subCursors) throws TddlException {

        List<Future<List<ISchematicCursor>>> futureCursors = new LinkedList<Future<List<ISchematicCursor>>>();

        /**
         * 将执行计划按group分组，组间并行执行，组内串行执行
         */
        Map<String, List<IDataNodeExecutor>> groupAndQcs = new HashMap();

        for (IDataNodeExecutor q : subNodes) {
            String groupName = q.getDataNode();

            List<IDataNodeExecutor> qcs = groupAndQcs.get(groupName);

            if (qcs == null) {
                qcs = new LinkedList();
                groupAndQcs.put(groupName, qcs);
            }

            qcs.add(q);
        }

        for (List<IDataNodeExecutor> qcs : groupAndQcs.values()) {
            Future<List<ISchematicCursor>> rcfuture = ExecutorContext.getContext()
                .getTopologyExecutor()
                .execByExecPlanNodesFuture(qcs, executionContext);
            futureCursors.add(rcfuture);
        }

        List<TddlException> exs = new ArrayList();
        for (Future<List<ISchematicCursor>> future : futureCursors) {
            try {
                subCursors.addAll(future.get(15, TimeUnit.MINUTES));
            } catch (Throwable e) {
                exs.add(new TddlException(e));
            }
        }

        if (GeneralUtil.isNotEmpty(exs)) {
            throw GeneralUtil.mergeException(exs);
        }
    }

    @SuppressWarnings("rawtypes")
    private void executeGroupConcurrent(ExecutionContext executionContext, List<IDataNodeExecutor> subNodes,
                                        List<ISchematicCursor> subCursors) throws TddlException {
        /**
         * group内串行，group间并行
         * 
         * <pre>
         *             group1    group2   group3
         * ---------------------------------------            
         * cursor0      t10       t20      t30
         * ---------------------------------------
         * cursor1      t11       t21      t31
         * ---------------------------------------
         * cursor2      t12       t22      t32
         * ---------------------------------------
         * 
         * 
         * </pre>
         */

        /**
         * 将执行计划按group分组，组间并行执行，组内串行执行
         */
        Map<String, List<IDataNodeExecutor>> groupAndQcs = new HashMap();

        for (IDataNodeExecutor q : subNodes) {
            String groupName = q.getDataNode();

            List<IDataNodeExecutor> qcs = groupAndQcs.get(groupName);

            if (qcs == null) {
                qcs = new LinkedList();
                groupAndQcs.put(groupName, qcs);
            }

            qcs.add(q);
        }
        int index = 0;
        while (true) {

            List<IDataNodeExecutor> oneConcurrentGroup = new ArrayList();
            for (List<IDataNodeExecutor> qcs : groupAndQcs.values()) {
                if (index >= qcs.size()) {
                    continue;
                }

                oneConcurrentGroup.add(qcs.get(index));
            }

            if (oneConcurrentGroup.isEmpty()) {
                break;
            }

            ConcurrentMergeCursor concurrentMergeCursor = new ConcurrentMergeCursor(oneConcurrentGroup,
                executionContext);

            subCursors.add(concurrentMergeCursor);

            if (index == 0) {
                concurrentMergeCursor.init();
            }
            index++;

        }

    }

    /**
     * 先进行合并，然后进行aggregats
     * 
     * @param cursor
     * @param context
     * @param executor
     * @param closeResultCursor
     * @param repo
     * @param executionContext
     * @return
     * @throws TddlException
     */
    private ISchematicCursor executeMergeAgg(ISchematicCursor cursor, IDataNodeExecutor executor,
                                             boolean closeResultCursor, IRepository repo, List<IOrderBy> groupBycols,
                                             ExecutionContext executionContext) throws TddlException {
        List _retColumns = ((IQueryTree) executor).getColumns();
        if (_retColumns != null) {
            List<IFunction> functionsNeedToCalculate = getFunctionsNeedToCalculate(_retColumns, true);

            if (((IMerge) executor).isDistinctByShardColumns()) {

                // distinct shard columns
                // do nothing
            } else {
                for (IFunction aggregate : functionsNeedToCalculate) {

                    if (aggregate.isNeedDistinctArg()) {
                        IQueryTree sub = (IQueryTree) ((IMerge) executor).getSubNodes().get(0);

                        // 这时候的order by是sub对外的order by，要做好别名替换
                        List<ISelectable> columns = ExecUtils.copySelectables(sub.getColumns());
                        for (ISelectable c : columns) {
                            c.setTableName(sub.getAlias());
                            if (c.getAlias() != null) {
                                c.setColumnName(c.getAlias());
                                c.setAlias(null);
                            }
                        }

                        // distinct 列的顺序可以随意
                        cursor = this.processOrderBy(cursor,
                            getOrderBy(columns),
                            executionContext,
                            (IQueryTree) executor,
                            false);
                        cursor = new DistinctCursor(cursor, cursor.getOrderBy());
                        break;
                    }
                }
            }

            if ((functionsNeedToCalculate != null && !functionsNeedToCalculate.isEmpty())
                || (groupBycols != null && !groupBycols.isEmpty())) {
                cursor = repo.getCursorFactory().aggregateCursor(executionContext,
                    cursor,
                    functionsNeedToCalculate,
                    groupBycols,
                    _retColumns,
                    true);
            }
        }
        return cursor;
    }

    @Override
    protected ISchematicCursor executeAgg(ISchematicCursor cursor, IDataNodeExecutor executor,
                                          boolean closeResultCursor, IRepository repo, List<IFunction> aggregates,
                                          List<IOrderBy> groupBycols, ExecutionContext executionContext)
                                                                                                        throws TddlException {
        return this.executeMergeAgg(cursor, executor, closeResultCursor, repo, groupBycols, executionContext);
    }

    @Override
    protected ISchematicCursor processOrderBy(ISchematicCursor cursor, List<IOrderBy> ordersInRequest,
                                              ExecutionContext executionContext, IQueryTree query,
                                              boolean needOrderMatch) throws TddlException {
        IRepository repo = executionContext.getCurrentRepository();
        // TODO shenxun: 临时表问题修复
        boolean hasOrderBy = ordersInRequest != null && !ordersInRequest.isEmpty();
        if (!hasOrderBy) {
            return cursor;
        }
        if (cursor instanceof IMergeCursor) {
            IMergeCursor mergeCursor = (IMergeCursor) cursor;
            List<ISchematicCursor> cursors = mergeCursor.getSubCursors();
            /*
             * 所有子节点，如果都是顺序，则可以直接使用mergeSort进行合并排序。 如果都是
             * 逆序，则不符合预期，用临时表（因为优化器应该做优化，尽可能将子cursor的顺序先变成正续，这里不会出现这种情况）
             * 如果有正有逆序，使用临时表 其他情况，使用临时表
             */
            OrderByResult tempOBR = null;
            for (ISchematicCursor subCur : cursors) {
                OrderByResult obR = chooseOrderByMethod(subCur, ordersInRequest, executionContext, needOrderMatch);
                if (tempOBR == null) {
                    tempOBR = obR;
                }
                if (obR != OrderByResult.normal) {
                    tempOBR = OrderByResult.temporaryTable;
                }

                break;

            }

            IMerge merge = (IMerge) query;

            if (tempOBR == OrderByResult.normal) {// 正常的合并.

                if (!(merge.getQueryConcurrency() == QUERY_CONCURRENCY.CONCURRENT)) {
                    tempOBR = OrderByResult.temporaryTable;

                }
            }

            if (tempOBR == OrderByResult.normal) {// 正常的合并.
                // 不去重的
                cursor = buildMergeSortCursor(executionContext, repo, cursors, true);
            } else if (tempOBR == OrderByResult.temporaryTable || tempOBR == OrderByResult.reverseCursor) {

                cursor = repo.getCursorFactory().tempTableSortCursor(executionContext,
                    cursor,
                    ordersInRequest,
                    true,
                    query.getRequestId());

            } else {
                throw new IllegalArgumentException("shoult not be here:" + tempOBR);
            }
        } else {
            return super.processOrderBy(cursor, ordersInRequest, executionContext, query, needOrderMatch);

        }
        return cursor;
        /*
         * cursor = repo.getCursorFactory().heapSortCursor( (ISchematicCursor)
         * cursor, _orderBy, _from, _from + _limit);
         */
    }

    private ISchematicCursor buildMergeSortCursor(ExecutionContext executionContext, IRepository repo,
                                                  List<ISchematicCursor> cursors, boolean duplicated)
                                                                                                     throws TddlException {
        ISchematicCursor cursor;
        cursor = repo.getCursorFactory().mergeSortedCursor(executionContext, cursors, duplicated);
        return cursor;
    }

    /**
     * <pre>
     * group by和aggregate Function。 
     * 对单机来说，原则就是尽可能的使用索引完成count max min的功能。
     * 参考的关键条件有：
     * 1. 是否需要group by 
     * 2. 是什么aggregate. 
     * 3. 是否需要distinct 
     * 4. 是否是merge节点
     * </pre>
     */
    @Override
    protected ISchematicCursor processGroupByAndAggregateFunction(ISchematicCursor cursor, IQueryTree query,
                                                                  ExecutionContext executionContext)
                                                                                                    throws TddlException {

        IMerge merge = (IMerge) query;
        // 是否带有group by 列。。
        List<IOrderBy> groupBycols = merge.getGroupBys();
        boolean closeResultCursor = executionContext.isCloseResultSet();
        final IRepository repo = executionContext.getCurrentRepository();

        List retColumns = getEmptyListIfRetColumnIsNull(merge);
        List<IFunction> _agg = getFunctionsNeedToCalculate(retColumns, true);
        // 接着处理group by
        if (groupBycols != null && !groupBycols.isEmpty()) {

            if (merge.isGroupByShardColumns()) {

                // group by sharding column
                // do nothing
            } else {
                // group by之前需要进行排序，按照group by列排序
                cursor = processOrderBy(cursor, (groupBycols), executionContext, merge, false);
            }
        }

        cursor = executeAgg(cursor, merge, closeResultCursor, repo, _agg, groupBycols, executionContext);
        return cursor;
    }
}
