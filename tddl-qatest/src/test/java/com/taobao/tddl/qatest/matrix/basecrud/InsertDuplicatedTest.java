package com.taobao.tddl.qatest.matrix.basecrud;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * insert重复数据
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class InsertDuplicatedTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public InsertDuplicatedTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void initData() throws Exception {
        tddlUpdateData("delete from  " + normaltblTableName, null);
        mysqlUpdateData("delete from  " + normaltblTableName, null);
    }

    /**
     * insert所有字段
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void insertAllFieldTest() throws Exception {

        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add(name);
        param.add(fl);
        execute(sql, param);

        sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE  name=?";
        param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add(name);
        param.add(fl);
        param.add("kkkk");
        execute(sql, param);

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * insert所有字段 带now()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void insertFunction() throws Exception {

        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add(name);
        param.add(fl);
        execute(sql, param);

        sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE  name=now()";
        param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add(name);
        param.add(fl);
        // param.add("kkkk");
        execute(sql, param);

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * insert所有字段 update中带两个字段，其中一个带now()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void insertFunctions() throws Exception {

        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add(name);
        param.add(fl);
        execute(sql, param);

        sql = "insert into " + normaltblTableName
              + " values(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE  name=?,gmt_create=now()";
        param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add(name);
        param.add(fl);
        param.add("kkkk");
        execute(sql, param);

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * batch insert所有字段, update中带两个字段，其中一个带now()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void insertBatchDuplicate() throws Exception {

        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(i);
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add(name);
            param.add(fl);

            params.add(param);
        }
        executeBatch(sql, params);

        sql = "insert into " + normaltblTableName
              + " values(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE  name=?,gmt_create=now()";

        params = new ArrayList();

        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(i);
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add(name);
            param.add(fl);
            param.add(name + i);
            params.add(param);
        }
        executeBatch(sql, params);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

}
