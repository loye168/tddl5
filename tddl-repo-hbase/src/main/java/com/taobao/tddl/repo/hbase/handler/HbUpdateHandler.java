package com.taobao.tddl.repo.hbase.handler;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.exception.ExecutorException;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.handler.PutHandlerCommon;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.plan.IPut;

public class HbUpdateHandler extends PutHandlerCommon {

    public HbUpdateHandler(){
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected int executePut(ExecutionContext executionContext, IPut put, ITable table, IndexMeta meta)
                                                                                                       throws Exception {
        int affect_rows = 0;
        IPut update = put;
        ISchematicCursor conditionCursor = null;
        try {
            conditionCursor = ExecutorContext.getContext()
                .getTopologyExecutor()
                .execByExecPlanNode(update.getQueryTree(), executionContext);
            IRowSet kv = null;
            IndexMeta primaryMeta = table.getSchema().getPrimaryIndex();

            // hbase update only update part,not all
            List<ColumnMeta> updateColumnMeta = new ArrayList<ColumnMeta>(update.getUpdateColumns().size());
            for (int i = 0; i < update.getUpdateColumns().size(); i++) {
                IColumn com = ExecUtils.getColumn(update.getUpdateColumns().get(i));
                for (ColumnMeta vcom : primaryMeta.getValueColumns()) {
                    if (vcom.getName().equals(com.getColumnName())) {
                        updateColumnMeta.add(vcom);
                    }
                }
            }

            CloneableRecord key = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
                .getCodec((primaryMeta.getKeyColumns()))
                .newEmptyRecord();

            CloneableRecord value = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
                .getCodec(updateColumnMeta)
                .newEmptyRecord();

            while ((kv = conditionCursor.next()) != null) {
                affect_rows++;
                for (int i = 0; i < update.getUpdateColumns().size(); i++) {
                    String cname = ExecUtils.getColumn(update.getUpdateColumns().get(i)).getColumnName();

                    Object v = update.getUpdateValues().get(i);
                    if (v instanceof IFunction) {
                        if (((IFunction) v).getFunctionType().equals(FunctionType.Aggregate)) {
                            throw new ExecutorException("update is not support aggregate function");
                        }
                        IFunction func = ((IFunction) v);

                        v = ((ScalarFunction) func.getExtraFunction()).scalarCalucate((IRowSet) null, executionContext);

                    }

                    value.put(cname, v);
                }

                for (ColumnMeta cm : primaryMeta.getKeyColumns()) {
                    Object val = getValByColumnMeta(kv, cm);
                    key.put(cm.getName(), val);
                    for (int i = 0; i < update.getUpdateColumns().size(); i++) {
                        String cname = ExecUtils.getColumn(ExecUtils.getColumn(update.getUpdateColumns().get(i)))
                            .getColumnName();
                        Object v = update.getUpdateValues().get(i);
                        if (cm.getName().equals(cname)) {
                            key.put(cname, v);
                        }
                    }
                }

                table.put(null, key, value, meta, put.getTableName());
            }

        } catch (Exception ex) {
            if (conditionCursor != null) {
                List<TddlException> exs = conditionCursor.close(null);
                if (!exs.isEmpty()) throw GeneralUtil.mergeException(exs);
            }
            throw ex;
        } finally {
            List<TddlException> exs = conditionCursor.close(null);
            if (!exs.isEmpty()) throw GeneralUtil.mergeException(exs);
        }
        return affect_rows;

    }

    private Object getValByColumnMeta(IRowSet kv, ColumnMeta cm) {
        Object val = ExecUtils.getValueByTableAndName(kv, cm.getTableName(), cm.getName(), cm.getAlias());
        return val;
    }

}
