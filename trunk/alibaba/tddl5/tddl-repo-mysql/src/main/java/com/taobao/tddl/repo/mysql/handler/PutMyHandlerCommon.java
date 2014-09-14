package com.taobao.tddl.repo.mysql.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.handler.HandlerCommon;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.IDataSourceGetter;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.monitor.Monitor;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.repo.mysql.spi.DatasourceMySQLImplement;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;
import com.taobao.tddl.repo.mysql.spi.My_Repository;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public abstract class PutMyHandlerCommon extends HandlerCommon {

    public PutMyHandlerCommon(){
        super();
        dsGetter = new DatasourceMySQLImplement();
    }

    protected IDataSourceGetter dsGetter;

    public Logger               logger = LoggerFactory.getLogger(PutMyHandlerCommon.class);

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        long time = System.currentTimeMillis();
        IPut put = (IPut) executor;
        My_JdbcHandler jdbcHandler = ((My_Repository) executionContext.getCurrentRepository()).getJdbcHandler(dsGetter,
            executor,
            executionContext);
        TableAndIndex ti = new TableAndIndex();

        buildTableAndMeta(put, ti, executionContext);
        ITable table = ti.table;
        IndexMeta meta = ti.index;

        ISchematicCursor result = null;
        try {
            result = executePut(executionContext, put, table, meta, jdbcHandler);
        } catch (Exception e) {
            time = Monitor.monitorAndRenewTime(Monitor.KEY1, Monitor.KEY2_TDDL_EXECUTE, Monitor.Key3Fail, time);
            throw new TddlException(e);
        }
        time = Monitor.monitorAndRenewTime(Monitor.KEY1, Monitor.KEY2_TDDL_EXECUTE, Monitor.Key3Success, time);
        return result;

    }

    protected abstract ISchematicCursor executePut(ExecutionContext executionContext, IPut put, ITable table,
                                                   IndexMeta meta, My_JdbcHandler myJdbcHandler) throws Exception;

    protected void prepare(ITransaction transaction, ITable table, IRowSet oldkv, CloneableRecord key,
                           CloneableRecord value, IPut.PUT_TYPE putType) throws TddlException {
    }

}
