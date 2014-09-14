package com.taobao.tddl.repo.hbase.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.RangeCursor;
import com.taobao.tddl.executor.exception.ExecutorException;
import com.taobao.tddl.executor.handler.QueryHandlerCommon;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode.FilterType;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 用于处理执行的handler 。 目的是将执行计划中的Query节点进行转义处理。
 * 这个执行器主要是用在使用KV接口的数据库里，比如bdb.concurrentHashMap等。
 * 
 * @author Whisper
 */
public class HbQueryHandler extends QueryHandlerCommon {

    public HbQueryHandler(){
    }

    @Override
    protected ISchematicCursor doQuery(ISchematicCursor cursor,

    IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        List<IOrderBy> _orderBy = ((IQueryTree) executor).getOrderBys();
        IRepository repo = executionContext.getCurrentRepository();
        IDataNodeExecutor _subQuery = null;
        ITransaction transaction = executionContext.getTransaction();
        IQuery query = (IQuery) executor;
        _subQuery = query.getSubQuery();
        IndexMeta meta = null;
        if (_subQuery != null) {
            // 如果有subQuery,则按照subquery构建
            cursor = ExecutorContext.getContext().getTopologyExecutor().execByExecPlanNode(_subQuery, executionContext);
        } else {
            ITable table = null;
            String indexName = query.getIndexName();

            TableAndIndex ti = new TableAndIndex();
            buildTableAndMeta(query, ti, executionContext);
            table = ti.table;
            meta = ti.index;

            if (cursor == null) {
                if (meta != null) {
                    cursor = table.getCursor(executionContext, meta, (IQuery) executor);
                    // cursor = repo.getCursorFactory().aliasCursor(cursor,
                    // table.getSchema().getTableName());
                } else {
                    throw new ExecutorException("index meta is null" + indexName);
                }
            }
        }
        // 获得查询结果的元数据
        // 获得本次查询的keyFilter
        IFilter keyFilter = query.getKeyFilter();

        if (keyFilter != null) {
            // 這裡hbase不支持skip index,必須是前綴的,所以這裡要重新選keyFilter
            Map<FilterType, IFilter> filters = this.getKeyAndValueFilter(keyFilter, meta);
            keyFilter = filters.get(FilterType.IndexQueryKeyFilter);
            IFilter valueFilter = filters.get(FilterType.IndexQueryValueFilter);

            query.setValueFilter(FilterUtils.and(valueFilter, query.getValueFilter()));

            query.setKeyFilter(keyFilter);
        }
        if (keyFilter != null) {
            if (keyFilter instanceof IBooleanFilter) {
                // 外键约束好像没用
                // if (meta.getRelationship() == Relationship.MANY_TO_MANY) {
                // cursor = manageToReverseIndex(cursor, executor, repo,
                // transaction, executionContext.getTable(), meta,
                // keyFilter);
                // } else {}
                // 非倒排索引,即普通索引,走的查询方式
                cursor = manageToBooleanRangeCursor(cursor, repo, keyFilter, executionContext);
            } else {
                cursor = repo.getCursorFactory().rangeCursor(executionContext, cursor, keyFilter);
            }

            if (cursor instanceof RangeCursor) {//

                if (_orderBy != null) {
                    if (_orderBy.size() == 1) {
                        IOrderBy o1 = _orderBy.get(0);
                        IOrderBy o2 = cursor.getOrderBy().get(0);
                        boolean needSort = !equalsIOrderBy(o1, o2);
                        boolean direction = o1.getDirection();
                        if (!needSort) {
                            if (!direction) {
                                // DescRangeCursor
                                cursor = repo.getCursorFactory().reverseOrderCursor(executionContext, cursor);
                            } else {
                                // asc,default
                            }
                            _orderBy = null;
                        }
                    }
                }
            }
        }
        return cursor;
    }

    protected ISchematicCursor manageToBooleanRangeCursor(ISchematicCursor cursor, IRepository repo, IFilter keyFilter,
                                                          ExecutionContext executionContext) throws TddlException {
        IBooleanFilter bf = (IBooleanFilter) keyFilter;
        IColumn c = ExecUtils.getColumn(bf.getColumn());
        OPERATION op = bf.getOperation();
        if (op == OPERATION.IN) {
            List<Object> values = bf.getValues();
            if (values == null) {
                throw new IllegalArgumentException("values is null ,but operation is in . logical error");
            } else {
                return repo.getCursorFactory().inCursor(executionContext, cursor, cursor.getOrderBy(), c, values, op);
            }
        }
        try {
            cursor = repo.getCursorFactory().rangeCursor(executionContext, cursor, keyFilter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return cursor;
    }

    /**
     * 返回结果的List<IFilter> 0是keyFilter，1是valueFilter
     * 
     * @param DNFNode
     * @param indexs
     * @param tablename
     * @return
     */
    public Map<FilterType, IFilter> getKeyAndValueFilter(IFilter filter, IndexMeta index) {
        List<IFilter> DNFNode = FilterUtils.toDNFNode(filter);
        Map<FilterType, IFilter> filters = new HashMap();
        Map<Object, List<IFilter>> columnAndItsFilters = FilterUtils.toColumnFiltersMap(DNFNode);

        List<ISelectable> indexKeyColumns = index == null ? new ArrayList<ISelectable>(0) : (OptimizerUtils.columnMetaListToIColumnList(index.getKeyColumns(),
            index.getName()));

        List<IFilter> indexQueryKeyFilters = new LinkedList<IFilter>();
        List<IFilter> indexQueryValueFilters = new LinkedList<IFilter>();

        // hbase索引需要前缀匹配
        for (int i = 0; i < indexKeyColumns.size(); i++) {

            boolean range = false;

            List<IFilter> fs = columnAndItsFilters.get(indexKeyColumns.get(i));
            List<IFilter> keyFilter = new ArrayList();
            if (fs != null) {
                for (IFilter f : fs) {

                    // filter右边不是常量的，不能走索引
                    if (((IBooleanFilter) f).getValue() != null
                        && (((IBooleanFilter) f).getValue() instanceof ISelectable)) {
                        continue;
                    }

                    // 范围条件一个rowkey只能有一个,之后的列无法走索引了
                    switch (f.getOperation()) {
                        case EQ:
                            keyFilter.add(f);
                            break;
                        case LT:
                        case LT_EQ:
                        case GT:
                        case GT_EQ:
                            keyFilter.add(f);
                            range = true;
                            break;
                        default:
                            break;
                    }
                }
                fs.clear();
            }
            indexQueryKeyFilters.addAll(keyFilter);
            // 不是前缀索引,不能用
            if (keyFilter.isEmpty()) break;
            if (range) break;

        }

        DNFNode.removeAll(indexQueryKeyFilters);

        IFilter indexQueryKeyTree = FilterUtils.DNFToAndLogicTree(indexQueryKeyFilters);
        IFilter indexQueryValueTree = FilterUtils.DNFToAndLogicTree(DNFNode);

        filters.put(FilterType.IndexQueryValueFilter, indexQueryValueTree);
        filters.put(FilterType.IndexQueryKeyFilter, indexQueryKeyTree);

        return filters;
    }

}
