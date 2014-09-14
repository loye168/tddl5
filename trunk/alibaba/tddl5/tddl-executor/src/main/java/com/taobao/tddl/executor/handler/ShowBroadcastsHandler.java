package com.taobao.tddl.executor.handler;

import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ArrayResultCursor;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.rule.TableRule;

/**
 * 返回一个表的拓扑信息
 * 
 * @author mengshi.sunmengshi 2014年5月9日 下午5:27:06
 * @since 5.1.0
 */
public class ShowBroadcastsHandler extends HandlerCommon {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        ArrayResultCursor result = new ArrayResultCursor("BROADCASTS", executionContext);
        result.addColumn("ID", DataType.IntegerType);
        result.addColumn("TABLE_NAME", DataType.StringType);
        result.initMeta();
        int index = 0;

        List<TableRule> tables = OptimizerContext.getContext().getRule().getTableRules();
        for (TableRule table : tables) {
            if (table.isBroadcast()) {
                result.addRow(new Object[] { index++, table.getVirtualTbName() });
            }
        }

        return result;

    }
}
