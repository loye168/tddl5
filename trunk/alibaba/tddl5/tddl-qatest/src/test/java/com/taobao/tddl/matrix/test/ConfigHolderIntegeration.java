package com.taobao.tddl.matrix.test;

import java.util.concurrent.Executors;

import org.junit.Ignore;
import org.junit.Test;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.MatrixExecutor;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.matrix.config.MatrixConfigHolder;

@Ignore("实际业务用例")
public class ConfigHolderIntegeration {

    @Test
    public void initTestWithConfigHolder() throws TddlException {
        MatrixConfigHolder configHolder = new MatrixConfigHolder();
        configHolder.setAppName("DEV_SUBWAY_MYSQL");
        // configHolder.setTopologyFilePath("test_matrix.xml");
        // configHolder.setSchemaFilePath("test_schema.xml");

        try {
            configHolder.init();
        } catch (TddlException e) {
            e.printStackTrace();
        }

        MatrixExecutor me = new MatrixExecutor();

        ExecutionContext context = new ExecutionContext();
        context.setExecutorService(Executors.newCachedThreadPool());
        // ResultCursor rc = me.execute("select * from bmw_users limit 10",
        // context);
        ResultCursor rc = me.execute("select * from lunaadgroup limit 10", context);
        IRowSet row = null;
        while ((row = rc.next()) != null) {
            System.out.println(row);
        }

        System.out.println("ok");
    }

}
