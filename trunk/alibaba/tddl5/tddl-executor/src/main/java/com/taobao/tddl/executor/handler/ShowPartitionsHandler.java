package com.taobao.tddl.executor.handler;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ArrayResultCursor;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.plan.bean.ShowWithTable;
import com.taobao.tddl.optimizer.core.plan.query.IShow;
import com.taobao.tddl.rule.TableRule;

/**
 * 返回一个表的分区字段信息
 * 
 * @author jianghang 2014-5-13 下午10:45:52
 * @since 5.1.0
 */
public class ShowPartitionsHandler extends BaseShowHandler {

    @Override
    public ISchematicCursor doShow(IShow executor, ExecutionContext executionContext) throws TddlException {
        ShowWithTable show = (ShowWithTable) executor;
        String tableName = show.getTableName();
        ArrayResultCursor result = new ArrayResultCursor("PARTITIONS", executionContext);
        result.addColumn("KEYS", DataType.StringType);
        result.initMeta();
        TableRule tableRule = OptimizerContext.getContext().getRule().getTableRule(tableName);
        if (tableRule == null) {
            result.addRow(new Object[] { "null" });
        } else {
            String keys = StringUtils.join(tableRule.getShardColumns(), ",");
            result.addRow(new Object[] { keys });
        }
        return result;

    }
}
