package com.taobao.tddl.executor.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.spi.ICommandHandler;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * @author mengshi.sunmengshi 2013-12-5 上午11:04:35
 * @since 5.0.0
 */
public abstract class HandlerCommon implements ICommandHandler {

    public static class TableAndIndex {

        public ITable    table;
        public IndexMeta index;
    }

    protected TableMeta getTableMeta(String tableName) {
        TableMeta ts = OptimizerContext.getContext().getSchemaManager().getTable(tableName);
        return ts;
    }

    protected void buildTableAndMeta(IPut put, TableAndIndex tableAndIndex, ExecutionContext executionContext)
                                                                                                              throws TddlException {
        String indexName = put.getIndexName();
        String groupDataNode = put.getDataNode();
        nestBuildTableAndSchema(groupDataNode, tableAndIndex, executionContext, indexName, put.getTableName(), true);
    }

    protected void buildTableAndMeta(IQuery query, TableAndIndex tableAndIndex, ExecutionContext executionContext)
                                                                                                                  throws TddlException {
        String indexName = query.getIndexName();
        String groupDataNode = query.getDataNode();

        nestBuildTableAndSchema(groupDataNode, tableAndIndex, executionContext, indexName, query.getTableName(), true);
    }

    /**
     * 取逻辑indexKey,而非实际index
     * 
     * @param query
     * @param executionContext
     * @throws Exception
     */
    protected void buildTableAndMetaLogicalIndex(IQuery query, TableAndIndex tableAndIndex,
                                                 ExecutionContext executionContext) throws TddlException {
        String indexName = query.getIndexName();
        String groupDataNode = query.getDataNode();
        nestBuildTableAndSchema(groupDataNode, tableAndIndex, executionContext, indexName, query.getTableName(), true);
    }

    /**
     * 准备indexMeta和ITable信息
     * 
     * @param tableAndIndex
     */
    protected void nestBuildTableAndSchema(String groupDataNode, TableAndIndex tableAndIndex,
                                           ExecutionContext executionContext, String indexName, String actualTable,
                                           boolean logicalIndex) throws TddlException {
        if (indexName != null && !"".equals(indexName)) {
            String tableName = ExecUtils.getLogicTableName(indexName);
            TableMeta ts = getTableMeta(tableName);
            tableAndIndex.index = ts.getIndexMeta(indexName);
            tableAndIndex.table = executionContext.getCurrentRepository().getTable(ts, groupDataNode, actualTable);
        }
    }

    public HandlerCommon(){
        super();
    }
}
