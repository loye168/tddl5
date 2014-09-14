package com.taobao.tddl.executor.handler;

import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ArrayResultCursor;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IShowWithTable;
import com.taobao.tddl.rule.model.TargetDB;

/**
 * 返回一个表的拓扑信息
 * 
 * @author mengshi.sunmengshi 2014年5月9日 下午5:27:06
 * @since 5.1.0
 */
public class ShowTopologyHandler extends HandlerCommon {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        IShowWithTable show = (IShowWithTable) executor;
        String tableName = show.getTableName();
        List<TargetDB> topology = OptimizerContext.getContext().getRule().shard(tableName, (IFilter) null, false, true);
        ArrayResultCursor result = new ArrayResultCursor("TOPOLOGY", executionContext);
        result.addColumn("ID", DataType.IntegerType);
        result.addColumn("GROUP_NAME", DataType.StringType);
        result.addColumn("TABLE_NAME", DataType.StringType);
        result.initMeta();
        int index = 0;
        for (TargetDB db : topology) {
            for (String tn : db.getTableNames())
                result.addRow(new Object[] { index++, db.getDbIndex(), tn });
        }

        return result;
    }
}
