package com.taobao.tddl.executor.handler;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IPut;

public class DeleteHandler extends PutHandlerCommon {

    public DeleteHandler(){
        super();
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected int executePut(ExecutionContext executionContext, IPut put, ITable table, IndexMeta meta)
                                                                                                       throws Exception {
        // ITransaction transaction = executionContext.getTransaction();
        int affect_rows = 0;
        IPut delete = put;
        ISchematicCursor conditionCursor = null;
        IRowSet rowSet = null;

        try {
            conditionCursor = ExecutorContext.getContext()
                .getTopologyExecutor()
                .execByExecPlanNode(delete.getQueryTree(), executionContext);

            List<CloneableRecord> keysToDelete = new ArrayList();
            while ((rowSet = conditionCursor.next()) != null) {
                CloneableRecord key = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
                    .getCodec(meta.getKeyColumns())
                    .newEmptyRecord();
                affect_rows++;
                for (ColumnMeta cm : meta.getKeyColumns()) {
                    Object val = getValByColumnMeta(rowSet, cm);
                    key.put(cm.getName(), val);
                }

                keysToDelete.add(key);

            }

            for (CloneableRecord key : keysToDelete) {
                table.delete(executionContext, key, meta, put.getTableName());
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if (conditionCursor != null) {
                List<TddlException> exs = new ArrayList();
                exs = conditionCursor.close(exs);
                if (!exs.isEmpty()) {
                    throw GeneralUtil.mergeException(exs);
                }
            }
        }
        return affect_rows;
    }

    private Object getValByColumnMeta(IRowSet kv, ColumnMeta cm) {
        Object val = ExecUtils.getValueByTableAndName(kv, cm.getTableName(), cm.getName(), cm.getAlias());
        return val;
    }

}
