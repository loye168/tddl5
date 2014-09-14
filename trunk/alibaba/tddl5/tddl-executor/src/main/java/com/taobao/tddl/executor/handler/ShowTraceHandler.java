package com.taobao.tddl.executor.handler;

import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ArrayResultCursor;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.statistics.SQLOperation;

/**
 * 返回一个表的拓扑信息
 * 
 * @author mengshi.sunmengshi 2014年5月9日 下午5:27:06
 * @since 5.1.0
 */
public class ShowTraceHandler extends HandlerCommon {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        ArrayResultCursor result = new ArrayResultCursor("TRACE", executionContext);
        result.addColumn("ID", DataType.IntegerType);
        result.addColumn("TYPE", DataType.StringType);
        result.addColumn("SQL/Result", DataType.StringType);
        result.addColumn("PARAMS", DataType.StringType);
        // result.addColumn("TRACE", DataType.StringType);
        result.initMeta();
        int index = 0;

        List<SQLOperation> ops = null;

        if (executionContext.getTracer() != null) {
            ops = executionContext.getTracer().getOperations();
            for (SQLOperation op : ops) {

                result.addRow(new Object[] { index++, op.getOperationType(), op.getSqlOrResult(), op.getParamsStr() });

            }
        }

        return result;

    }
}
