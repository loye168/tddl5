package com.taobao.tddl.qatest.matrix.basecrud;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * batch insert 测试，mysql与tddl同时入库，入库结束后与mysql中内容相比较
 * 
 * @author zhuoxue
 * @since 5.0.1
 */

@RunWith(EclipseParameterized.class)
public class BatchInsertTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public BatchInsertTest(String tableName){
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
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add(name);
            param.add(fl);

            params.add(param);
        }
        executeBatch(sql, params);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * insert ignore所有字段
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void insertAllFieldIgnoreTest() throws Exception {

        if (normaltblTableName.startsWith("ob")) {
            // ob不支持
            return;
        }
        String sql = "insert ignore into " + normaltblTableName + " values(?,?,?,?,?,?,?)";
        List<List<Object>> params = new ArrayList();
        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(1);
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add(name);
            param.add(fl);

            params.add(param);
        }
        executeBatch(sql, params);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * insert所有字段其中带now()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void insertAllFieldTestWithFunction() throws Exception {
        String sql = "insert into " + normaltblTableName + " values(?,?,now(),?,?,?,?)";

        List<List<Object>> params = new ArrayList();
        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(gmt);
            param.add(gmt);
            param.add(name);
            param.add(fl);
            params.add(param);
        }
        executeBatch(sql, params);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = { "PK", "ID", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 有部分列没有使用绑定变量，会造成顺序不一致，测试此时的映射情况
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void insertAllFieldTestWithSomeFieldCostants() throws Exception {
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,'123',?)";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            // param.add(name);
            param.add(fl);

            params.add(param);
        }
        executeBatch(sql, params);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * insert所有字段其中带常量和sequence
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void insertAllFieldTestWithSomeFieldCostantsAndSeq() throws Exception {
        if (normaltblTableName.startsWith("ob_")) {
            return;
        }
        String tddlSql = "insert into " + normaltblTableName
                         + "(pk,id,gmt_create,gmt_timestamp,gmt_datetime,name,floatCol) values(" + normaltblTableName
                         + ".nextval,?,?,?,?,'123',?)";

        String mysqlSql = "insert into " + normaltblTableName
                          + "(pk,id,gmt_create,gmt_timestamp,gmt_datetime,name,floatCol) values(?,?,?,?,?,'123',?)";

        List<List<Object>> tddlParams = new ArrayList();
        List<List<Object>> mysqlParams = new ArrayList();

        long base = Long.valueOf(RandomStringUtils.randomNumeric(8));
        for (int i = 0; i < 50; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(base + i);
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            // param.add(name);
            param.add(fl);
            tddlParams.add(param);
        }

        for (int i = 0; i < 50; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(base + i);
            param.add(base + i);
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            // param.add(name);
            param.add(fl);
            mysqlParams.add(param);
        }

        mysqlUpdateDataBatch(mysqlSql, mysqlParams);
        tddlUpdateDataBatch(tddlSql, tddlParams);

        ResultSet rs = tddlPreparedStatement.getGeneratedKeys();
        while (rs.next()) {
            System.out.println("generated_keys : " + rs.getLong(1));
        }

        String sql = "select * from " + normaltblTableName;
        // 不检查pk，mysql和tddl自增id生成机制不一样
        String[] columnParam = { "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select last_insert_id() a";
        columnParam = new String[] { "a" };
        ResultSet rc = tddlQueryData(sql, null);
        while (rc.next()) {
            System.out.println("last_insert_id : " + rc.getLong("a"));
        }
    }
}
