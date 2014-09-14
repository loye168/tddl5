package com.taobao.tddl.repo.hbase.handler;

import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.handler.PutHandlerCommon;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IPut;

public class HbDeleteHandler extends PutHandlerCommon {

    public HbDeleteHandler(){
    }

    @Override
    protected int executePut(ExecutionContext executionContext, IPut put, ITable table, IndexMeta meta)
                                                                                                       throws Exception {
        int affect_rows = 0;
        IPut delete = put;
        ISchematicCursor conditionCursor = null;
        conditionCursor = ExecutorContext.getContext()
            .getTopologyExecutor()
            .execByExecPlanNode(put.getQueryTree(), executionContext);
        IRowSet rowSet = null;
        try {
            while ((rowSet = conditionCursor.next()) != null) {
                affect_rows++;
                CloneableRecord key = ExecUtils.convertToClonableRecord(rowSet);
                // prepare(transaction, table, rowSet, null, null,
                // PUT_TYPE.DELETE);
                table.delete(null, key, meta, put.getTableName());
            }
        } catch (Exception e) {
            throw e;
        } finally {
            List<TddlException> exs = conditionCursor.close(null);

            if (!exs.isEmpty()) throw GeneralUtil.mergeException(exs);
        }

        return affect_rows;
    }

}
