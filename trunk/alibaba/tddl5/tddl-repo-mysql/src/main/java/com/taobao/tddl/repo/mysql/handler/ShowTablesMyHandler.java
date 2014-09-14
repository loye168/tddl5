package com.taobao.tddl.repo.mysql.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ArrayResultCursor;
import com.taobao.tddl.executor.function.scalar.filter.Like;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.ExtraFunctionManager;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.bean.ShowWithoutTable;
import com.taobao.tddl.repo.mysql.spi.My_Cursor;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;
import com.taobao.tddl.repo.mysql.spi.My_Repository;

/**
 * 返回show tables结果，合并mysql和规则中的表信息
 * 
 * @author jianghang 2014-5-13 下午11:03:35
 * @since 5.1.0
 */
public class ShowTablesMyHandler extends QueryMyHandler {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        ShowWithoutTable show = (ShowWithoutTable) executor;
        boolean full = show.isFull();
        ArrayResultCursor result = new ArrayResultCursor("TABLES", executionContext);
        result.addColumn("Tables_in_" + executor.getDataNode(), DataType.StringType);
        if (full) {
            result.addColumn("Table_type", DataType.StringType);
        }
        result.initMeta();
        executor.setSql(executor.toString());// 设置下指定的sql
        ISchematicCursor cursor = null;
        List<String> defaultDbTables = new ArrayList<String>();
        Map<String, String> tableTypes = new HashMap<String, String>();
        try {
            My_JdbcHandler jdbcHandler = ((My_Repository) executionContext.getCurrentRepository()).getJdbcHandler(dsGetter,
                executor,
                executionContext);
            My_Cursor my_cursor = new My_Cursor(jdbcHandler, null, executor, executor.isStreaming());
            cursor = my_cursor.getResultSet();
            IRowSet row = null;
            while ((row = cursor.next()) != null) {
                String table = StringUtils.upperCase(row.getString(0));
                defaultDbTables.add(table);
                if (full) {
                    String type = row.getString(1);
                    tableTypes.put(table, type);
                }
            }
        } finally {
            // 关闭cursor
            if (cursor != null) {
                cursor.close(new ArrayList<TddlException>());
            }
        }

        Set<String> tableNames = OptimizerContext.getContext().getRule().mergeTableRule(defaultDbTables);
        // 去掉sequence表
        tableNames.remove("SEQUENCE");
        Like like = null;
        String likeExpr = null;
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
                String type = tableTypes.get(table);
                if (type == null) {
                    type = "BASE TABLE";
                }

                result.addRow(new Object[] { table, type });
            } else {
                result.addRow(new Object[] { table });
            }
        }
        return result;
    }
}
