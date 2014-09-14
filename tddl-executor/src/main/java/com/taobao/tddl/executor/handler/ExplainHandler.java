package com.taobao.tddl.executor.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ArrayResultCursor;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

/**
 * explain处理
 * 
 * @author jianghang 2014-6-24 下午1:45:56
 * @since 5.1.5
 */
public class ExplainHandler extends HandlerCommon {

    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        ArrayResultCursor result = new ArrayResultCursor("explain", executionContext);
        result.addColumn("GROUP_NAME", DataType.StringType);
        result.addColumn("SQL", DataType.StringType);
        result.addColumn("PARAMS", DataType.StringType);
        result.initMeta();

        result.addRow(new Object[] { executor.getDataNode(), executor.toStringWithInden(0, executor.getExplainMode()),
                null });
        return result;
    }

}
