package com.taobao.tddl.executor.handler;

import java.util.ArrayList;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ArrayResultCursor;
import com.taobao.tddl.executor.function.scalar.filter.Like;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.ExtraFunctionManager;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.bean.ShowWithoutTable;

/**
 * 返回show tables结果，默认只返回分库分表中的信息
 * 
 * @author jianghang 2014-5-13 下午10:45:52
 * @since 5.1.0
 */
public class ShowTablesHandler extends HandlerCommon {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        ShowWithoutTable show = (ShowWithoutTable) executor;
        ArrayResultCursor result = new ArrayResultCursor("TABLES", executionContext);
        result.addColumn("Tables_in_" + executor.getDataNode(), DataType.StringType);
        result.initMeta();

        Set<String> tableNames = OptimizerContext.getContext().getRule().mergeTableRule(new ArrayList<String>());
        Like like = null;
        String likeExpr = null;
        boolean full = show.isFull();
        if (StringUtils.isNotEmpty(show.getPattern())) {
            like = (Like) ExtraFunctionManager.getExtraFunction("LIKE");
            if (show.getPattern().charAt(0) == '\'' && show.getPattern().charAt(show.getPattern().length() - 1) == '\'') {
                likeExpr = show.getPattern().substring(1, show.getPattern().length() - 1);
            } else {
                likeExpr = show.getPattern();
            }
        }
        for (String table : tableNames) {
            if (like != null) {
                boolean bool = (Boolean) like.compute(new Object[] { table, likeExpr }, executionContext);
                if (!bool) {
                    continue;
                }
            }

            if (full) {
                String type = "BASE TABLE";
                result.addRow(new Object[] { table, type });
            } else {
                result.addRow(new Object[] { table });
            }
        }
        return result;
    }
}
