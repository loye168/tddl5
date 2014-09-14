package com.taobao.tddl.executor.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.TransactionConfig;
import com.taobao.tddl.executor.cursor.IAffectRowCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.exception.ExecutorException;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.repo.RepositoryConfig;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.monitor.Monitor;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;

/**
 * CUD操作基类
 * 
 * @since 5.0.0
 */
public abstract class PutHandlerCommon extends HandlerCommon {

    public PutHandlerCommon(){
        super();
    }

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {

        if (executionContext.getParams() != null && executionContext.getParams().isBatch()) {
            throw new ExecutorException("batch is not supported for :"
                                        + executionContext.getCurrentRepository().getClass());
        }

        long time = System.currentTimeMillis();
        IPut put = (IPut) executor;
        TableAndIndex ti = new TableAndIndex();
        buildTableAndMeta(put, ti, executionContext);

        int affect_rows = 0;
        ITransaction transaction = executionContext.getTransaction();
        ITable table = ti.table;
        IndexMeta meta = ti.index;

        try {
            if (transaction == null) {// 客户端没有用事务，这里手动加上。
                throw new IllegalAccessError("txn is null");
            }
            affect_rows = executePut(executionContext, put, table, meta);
        } catch (Exception e) {
            time = Monitor.monitorAndRenewTime(Monitor.KEY1, Monitor.KEY2_TDDL_EXECUTE, Monitor.Key3Fail, time);
            throw new TddlNestableRuntimeException(e);
        }

        // 这里返回key->value的方式的东西，类似Key=affectRow val=1 这样的软编码
        IAffectRowCursor affectrowCursor = executionContext.getCurrentRepository()
            .getCursorFactory()
            .affectRowCursor(executionContext, affect_rows);

        time = Monitor.monitorAndRenewTime(Monitor.KEY1, Monitor.KEY2_TDDL_EXECUTE, Monitor.Key3Success, time);
        return affectrowCursor;

    }

    protected abstract int executePut(ExecutionContext executionContext, IPut put, ITable table, IndexMeta meta)
                                                                                                                throws Exception;

    protected void prepare(ITransaction transaction, ITable table, IRowSet oldkv, CloneableRecord key,
                           CloneableRecord value, IPut.PUT_TYPE putType) throws TddlException {
    }

    protected TransactionConfig getDefalutTransactionConfig(IRepository repo) {
        TransactionConfig tc = new TransactionConfig();
        String isolation = repo.getRepoConfig().getProperty(RepositoryConfig.DEFAULT_TXN_ISOLATION);
        // READ_UNCOMMITTED|READ_COMMITTED|REPEATABLE_READ|SERIALIZABLE
        if ("READ_UNCOMMITTED".equals(isolation)) {
            tc.setReadUncommitted(true);
        } else if ("READ_COMMITTED".equals(isolation)) {
            tc.setReadCommitted(true);
        }
        return tc;
    }

}
