package com.taobao.tddl.repo.mysql.handler;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ArrayResultCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.bean.ShowWithTable;

public class ShowCreateTableMyHandler extends ShowDirectMyHandler {

    public ISchematicCursor doShow(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        ISchematicCursor cursor = null;
        try {
            ArrayResultCursor result = new ArrayResultCursor("Create Table", executionContext);
            result.addColumn("Table", DataType.StringType);
            result.addColumn("Create Table", DataType.StringType);
            result.initMeta();

            ShowWithTable show = (ShowWithTable) executor;
            cursor = super.doShow(executor, executionContext);
            IRowSet row = null;
            while ((row = cursor.next()) != null) {
                String table = StringUtils.lowerCase(row.getString(0));
                String sql = row.getString(1);
                result.addRow(new Object[] { show.getTableName(),
                        StringUtils.replaceOnce(sql, table, show.getTableName()) });
            }

            return result;
        } finally {
            // 关闭cursor
            if (cursor != null) {
                cursor.close(new ArrayList<TddlException>());
            }
        }

    }

    public static void main(String args[]) {
        String str = StringUtils.replaceOnce("CREATE TABLE `test_table_autoinc_onegroup_mutilatom_00` (",
            "test_table_autoinc_onegroup_mutilatom_00",
            "hello");
        System.out.println(str);
    }
}
