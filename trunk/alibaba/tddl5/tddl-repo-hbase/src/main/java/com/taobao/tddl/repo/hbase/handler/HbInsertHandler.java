package com.taobao.tddl.repo.hbase.handler;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
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

public class HbInsertHandler extends PutHandlerCommon {

    public HbInsertHandler(){
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected int executePut(ExecutionContext executionContext, IPut put, ITable table, IndexMeta meta)
                                                                                                       throws Exception {
        int affect_rows = 0;
        IPut insert = put;

        // hbase update only update part,not all
        List<ColumnMeta> updateColumnMeta = new ArrayList<ColumnMeta>(insert.getUpdateColumns().size());
        for (int i = 0; i < insert.getUpdateColumns().size(); i++) {
            IColumn com = ExecUtils.getColumn(insert.getUpdateColumns().get(i));
            for (ColumnMeta vcom : meta.getValueColumns()) {
                if (vcom.getName().equals(com.getColumnName())) {
                    updateColumnMeta.add(vcom);
                }
            }
        }

        CloneableRecord key = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
            .getCodec(meta.getKeyColumns())
            .newEmptyRecord();
        CloneableRecord value = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
            .getCodec(updateColumnMeta)
            .newEmptyRecord();
        List columns = insert.getUpdateColumns();
        L: for (int i = 0; i < columns.size(); i++) {
            for (ColumnMeta cm : meta.getKeyColumns()) {
                if (cm.getName().equals(ExecUtils.getColumn(columns.get(i)).getColumnName())) {
                    Object v = insert.getUpdateValues().get(i);
                    if (v instanceof IFunction) {
                        if (((IFunction) v).getFunctionType().equals(FunctionType.Aggregate)) {
                            throw new ExecutorException("insert is not support aggregate function");
                        }
                        IFunction func = ((IFunction) v);

                        v = ((ScalarFunction) func.getExtraFunction()).scalarCalucate((IRowSet) null, executionContext);

                    }
                    key.put(cm.getName(), v);
                    continue L;
                }
            }
            for (ColumnMeta cm : meta.getValueColumns()) {
                if (cm.getName().equals(ExecUtils.getColumn(columns.get(i)).getColumnName())) {
                    Object v = insert.getUpdateValues().get(i);
                    if (v instanceof IFunction) {
                        if (((IFunction) v).getFunctionType().equals(FunctionType.Aggregate)) {
                            throw new ExecutorException("insert is not support aggregate function");
                        }
                        IFunction func = ((IFunction) v);

                        v = ((ScalarFunction) func.getExtraFunction()).scalarCalucate((IRowSet) null, executionContext);

                    }
                    value.put(cm.getName(), v);
                    break;
                }
            }
        }

        // prepare(transaction, table, null, key, value, PUT_TYPE.INSERT);
        table.put(null, key, value, meta, put.getTableName());
        affect_rows++;
        return affect_rows;

    }

}
