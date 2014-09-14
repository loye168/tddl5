package com.taobao.tddl.qatest.matrix.basecrud;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * update测试
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class UpdateTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public UpdateTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void prepare() throws Exception {
        normaltblPrepare(0, 20);
    }

    /**
     * update所有数据
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateAll() throws Exception {

        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE " + normaltblTableName
                     + " SET id=?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=?";
        List<Object> param = new ArrayList<Object>();
        param.add(9999);
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add("new_name");
        param.add(0.999F);
        executeCountAssert(sql, param);

        sql = "SELECT * FROM " + normaltblTableName;
        String[] columnParam = { "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * update一条数据
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateOne() throws Exception {
        long pk = 5;
        String sql = "UPDATE " + normaltblTableName
                     + " SET id=?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=? WHERE pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(rand.nextInt());
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add("new_name" + rand.nextInt());
        param.add(fl);
        param.add(pk);
        executeCountAssert(sql, param);

        sql = "SELECT * FROM " + normaltblTableName + " WHERE pk=" + pk;
        String[] columnParam = { "ID", "GMT_CREATE", "GMT_TIMESTAMP", "GMT_DATETIME", "NAME", "FLOATCOL" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * update一部分数据，使用between
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateSome() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE "
                     + normaltblTableName
                     + "  SET id=?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=? WHERE pk BETWEEN 3 AND 7";
        List<Object> param = new ArrayList<Object>();
        param.add(rand.nextInt());
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add("new_name" + rand.nextInt());
        param.add(fl);
        executeCountAssert(sql, param);

        sql = "SELECT * FROM " + normaltblTableName + "  WHERE pk BETWEEN 3 AND 7";
        String[] columnParam = { "ID", "GMT_CREATE", "GMT_TIMESTAMP", "GMT_DATETIME", "NAME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * update一部分数据，使用pk > 7
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateSome1() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE " + normaltblTableName
                     + "  SET id=?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=? WHERE pk > 7";
        List<Object> param = new ArrayList<Object>();
        param.add(rand.nextInt());
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add("new_name" + rand.nextInt());
        param.add(fl);
        executeCountAssert(sql, param);

        sql = "SELECT * FROM " + normaltblTableName + "  WHERE pk > 7";
        String[] columnParam = { "ID", "GMT_CREATE", "GMT_TIMESTAMP", "GMT_DATETIME", "NAME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * update一部分数据，使用pk < 7
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateSome2() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE " + normaltblTableName
                     + "  SET id=?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=? WHERE pk < 7";
        List<Object> param = new ArrayList<Object>();
        param.add(rand.nextInt());
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add("new_name" + rand.nextInt());
        param.add(fl);
        executeCountAssert(sql, param);

        sql = "SELECT * FROM " + normaltblTableName + "  WHERE pk < 7";
        String[] columnParam = { "ID", "GMT_CREATE", "GMT_TIMESTAMP", "GMT_DATETIME", "NAME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * update一部分数据，使用pk >= 7
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateSome3() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE " + normaltblTableName + "  SET id=?,gmt_create=?,name=?,floatCol=? WHERE pk >= 7";
        List<Object> param = new ArrayList<Object>();
        param.add(rand.nextInt());
        param.add(gmtDay);
        param.add("new_name" + rand.nextInt());
        param.add(fl);
        executeCountAssert(sql, param);

        sql = "SELECT * FROM " + normaltblTableName + "  WHERE pk >= 7";
        String[] columnParam = { "ID", "GMT_CREATE", "NAME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * update一部分数据，使用pk <= 7
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateSome4() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE " + normaltblTableName + "  SET id=?,gmt_create=?,name=?,floatCol=? WHERE pk <= 7";
        List<Object> param = new ArrayList<Object>();
        param.add(rand.nextInt());
        param.add(gmtDay);
        param.add("new_name" + rand.nextInt());
        param.add(fl);
        executeCountAssert(sql, param);

        sql = "SELECT * FROM " + normaltblTableName + "  WHERE pk <= 7";
        String[] columnParam = { "ID", "GMT_CREATE", "NAME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * update 使用now()来赋值
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void nowTest() throws Exception {

        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "update " + normaltblTableName
                     + " set  gmt_create= now(),gmt_timestamp= now(),gmt_datetime=now() where pk=1";
        int mysqlRow = mysqlUpdateData(sql, null);
        int row = tddlUpdateData(sql, null);
        Assert.assertEquals(mysqlRow, row);

        sql = "select * from " + normaltblTableName + " where pk = 1";
        rs = mysqlQueryData(sql, null);
        rc = tddlQueryData(sql, null);
        String[] columnParam = { "gmt_create", "gmt_timestamp", "gmt_datetime" };
        assertOrder(rs, rc, columnParam);
    }

    /**
     * update 更新的条件中带&&和||条件
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void whereWithComplexTest() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "update " + normaltblTableName
                     + " set floatCol= ? where name= ? and ((?-id>100)||(?<id && ?-id >200))";
        List<Object> param = new ArrayList<Object>();
        param.add(1.2);
        param.add(name);
        param.add(400);
        param.add(200);
        param.add(800);
        executeCountAssert(sql, param);
    }

    /**
     * update 更新的值，自增：v=v+?
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void setWithIncrementTest() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "update " + normaltblTableName + " set floatCol= floatCol+ ? where id =? and name =?";
        List<Object> param = new ArrayList<Object>();
        param.add(2);
        param.add(200);
        param.add(name);
        executeCountAssert(sql, param);

        sql = "select floatCol from " + normaltblTableName + " where id =? and name=?";
        param.clear();
        param.add(200);
        param.add(name);
        String[] columnParam = { "floatCol" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * update 不存在的值
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateNotExistDataTest() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        long pk = -11l;
        String sql = "UPDATE " + normaltblTableName + "  SET id=? WHERE pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(rand.nextInt());
        param.add(pk);
        executeCountAssert(sql, param);
    }

    /**
     * update 不存在的列
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateNotExistFielddTest() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE " + normaltblTableName + "  SET nothisfield = ?";
        try {
            tddlUpdateData(sql, null);
            Assert.fail();
        } catch (Exception e) {
            // Assert.assertTrue(e.getMessage().contains(" NOTHISFIELD is not existed"));
        }
    }

    /**
     * update 不存在的表
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateNotExistTableTest() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE nor SET pk = ?";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        try {
            tddlUpdateData(sql, param);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage() != null);
        }
    }

    /**
     * update 字段与值的类型不匹配
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Ignore
    public void updateNotMatchTypeTest() throws Exception {
        if (normaltblTableName.startsWith("ob") || normaltblTableName.endsWith("oneGroup_oneAtom")) {
            // ob不支持批量更新
            return;
        }
        String sql = "UPDATE " + normaltblTableName + "  SET id=? WHERE pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add("NIHAO");
        param.add(1l);
        tddlUpdateData(sql, param);
    }

}
