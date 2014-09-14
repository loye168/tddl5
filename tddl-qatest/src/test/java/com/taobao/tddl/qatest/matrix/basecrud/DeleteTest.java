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
 * delete测试
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class DeleteTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public DeleteTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void prepare() throws Exception {
        tddlUpdateData("delete from " + normaltblTableName, null);
        normaltblPrepare(0, 20);
    }

    /**
     * delete all records
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void deleteAll() throws Exception {
        String sql = String.format("delete from %s", normaltblTableName);
        executeCountAssert(sql, Collections.EMPTY_LIST);
    }

    /**
     * delete one record
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void deleteOne() throws Exception {
        String sql = String.format("delete from %s where pk = ?", normaltblTableName);
        executeCountAssert(sql, Arrays.asList(new Object[] { 5L }));
    }

    /**
     * delete some records with between
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void deleteWithBetweenAnd() throws Exception {
        String sql = String.format("DELETE FROM %s WHERE pk BETWEEN 2 AND 7", normaltblTableName);
        executeCountAssert(sql, Collections.EMPTY_LIST);
    }

    /**
     * delete some records 条件为 where pk =2 or pk=7
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void deleteWithOrTest() throws Exception {
        String sql = String.format("delete from %s where pk =2 or pk=7", normaltblTableName);
        executeCountAssert(sql, Collections.EMPTY_LIST);

        sql = String.format("select * from %s where pk=2 or pk=7", normaltblTableName);
        selectConutAssert(sql, Collections.EMPTY_LIST);
    }

    /**
     * delete some records 条件为 where pk in (2,7,10)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void deleteWithInTest() throws Exception {
        String sql = String.format("delete from %s where pk in (2,7,10)", normaltblTableName);
        executeCountAssert(sql, Collections.EMPTY_LIST);

        sql = String.format("select * from %s where pk=2 or pk=7 or pk=10", normaltblTableName);
        selectConutAssert(sql, Collections.EMPTY_LIST);
    }

    @Ignore("目前delete操作不支持后面跟order by ? limit ?操作")
    @Test
    public void deleteWithOrderByLimitTest() throws Exception {
        int limitNum = 1;
        String sql = String.format("delete from %s where name=? order by name limit ?", normaltblTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        param.add(limitNum);
        selectConutAssert(sql, param);
    }

    /**
     * delete some records 条件为 gmt_create < now()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void deleteWithNowTest() throws Exception {
        try {
            String sql = String.format("delete from %s where gmt_create < now()", normaltblTableName);
            executeCountAssert(sql, Collections.EMPTY_LIST);
            normaltblPrepare(0, 20);
            sql = String.format("delete from %s where gmt_timestamp < now()", normaltblTableName);
            executeCountAssert(sql, Collections.EMPTY_LIST);
            normaltblPrepare(0, 20);
            sql = String.format("delete from %s where gmt_datetime > now()", normaltblTableName);
            executeCountAssert(sql, Collections.EMPTY_LIST);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("delete中暂不支持按照索引进行查询"));
        }
    }

    /**
     * delete 不存在的数据
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void deleteNotExistTest() throws Exception {
        String sql = String.format("delete from  %s where pk= %s ", normaltblTableName, RANDOM_ID);
        tddlUpdateData(sql, null);
        mysqlUpdateData(sql, null);
        sql = String.format("delete from %s where pk =?", normaltblTableName);
        executeCountAssert(sql, Arrays.asList(new Object[] { RANDOM_ID }));
    }

    /**
     * delete 不存在的表
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void deleteNotExistTableTest() throws Exception {
        String sql = String.format("delete from norma where pk =%s", RANDOM_ID);
        try {
            tddlUpdateData(sql, null);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage() != null);
        }
    }

    /**
     * delete 不存在的列
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void deleteNotExistFieldTest() throws Exception {
        String sql = String.format("delete from %s where k =%s", normaltblTableName, RANDOM_INT);
        try {
            tddlUpdateData(sql, null);
            Assert.fail();
        } catch (Exception ex) {
            // Assert.assertTrue(ex.getMessage().contains("column: K is not existed"));
        }
    }

    /**
     * delete条件中类型不匹配
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void deleteNotMacthTypeTest() throws Exception {
        if (!normaltblTableName.contains("mysql")) {
            String sql = String.format("delete from %s where pk = ? %s", normaltblTableName, RANDOM_INT);
            try {
                tddlUpdateData(sql, null);
                Assert.fail();
            } catch (Exception ex) {
                // Assert.assertTrue(ex.getMessage().contains("SQL syntax error"));
            }
        } else {
            try {
                String sql = String.format("delete from %s where pk =? %s ", normaltblTableName, RANDOM_INT);
                int row = tddlUpdateData(sql, null);
                Assert.assertEquals(0, row);
                Assert.fail();
            } catch (Exception e) {
                // Assert.assertTrue(e.getMessage().contains("SQL syntax error"));
            }
        }
    }

    /**
     * delete语句写错
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void deleteWrongSqlTest() throws Exception {
        String sql = String.format("delete from %s wheer pk =1", normaltblTableName);
        try {
            tddlUpdateData(sql, null);
            Assert.fail();
        } catch (Exception ex) {
            // Assert.assertTrue(ex.getMessage().contains("SQL syntax error"));
        }

        sql = String.format("delete form %s wheer pk=1", normaltblTableName);
        try {
            tddlUpdateData(sql, null);
            Assert.fail();
        } catch (Exception ex) {
            // Assert.assertTrue(ex.getMessage().contains("SQL syntax error"));
        }
    }
}
