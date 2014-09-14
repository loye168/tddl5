package com.taobao.tddl.qatest.matrix.select;

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
 * 子查询测试 >,>=,<,<=,=,!=,like all any
 * 
 * @author mengshi.sunmengshi 2014年4月8日 下午4:50:54
 * @since 5.1.0
 */
@RunWith(EclipseParameterized.class)
public class SelectWithSubqueryTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblStudentTable(dbType));
    }

    public SelectWithSubqueryTest(String normaltblTableName, String studentTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
        BaseTestCase.studentTableName = studentTableName;
    }

    @Before
    public void prepareDate() throws Exception {
        normaltblPrepare(0, 20);
        studentPrepare(0, MAX_DATA_SIZE);
    }

    /**
     * in
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void inTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk in (select id from " + studentTableName + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 等于子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void equalTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = (select id from " + studentTableName
                     + " order by id limit 1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 等于子查询中的最大值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void equalMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = (select max(id) from " + studentTableName
                     + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 大于子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void greaterTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk > (select id from " + studentTableName
                     + " order by id asc limit 5,1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 大于等于子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void greaterEqualTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk >= (select id from " + studentTableName
                     + " order by id asc limit 5,1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 小于子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void lessTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk < (select id from " + studentTableName
                     + " order by id asc limit 5,1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 小于等于子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void lessEqualTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk <= (select id from " + studentTableName
                     + " order by id asc limit 5,1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 不等于子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void notEqualTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk != (select id from " + studentTableName
                     + " order by id asc limit 5,1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * like子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void likeTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name like (select name from " + studentTableName
                     + " order by id asc limit 5,1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 大于any子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void greaterAnyTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk > any(select id from " + studentTableName + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 大于等于any子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void greaterEqualAnyTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk >= any(select id from " + studentTableName
                     + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 小于any子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void lessAnyTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk < any(select id from " + studentTableName + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 小于等于any子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void lessEqualAnyTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk <= any(select id from " + studentTableName
                     + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 不等于any子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void notEqualAnyTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk != any(select id from " + studentTableName
                     + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 不等于any子查询中的一个值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void notEqualAnyOneValueTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk != any(select id from " + studentTableName
                     + " where id=1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 等于any子查询中的值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void equalAnyTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = any(select id from " + studentTableName + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 等于any子查询中的最大值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void equalAnyMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 大于any子查询中的最大值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void greatAnyMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk > any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 大于any子查询中的最大值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void greatEqualAnyMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk >= any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 小于any子查询中的最大值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void lessAnyMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk < any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 小于等于any子查询中的最大值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void lessEqualAnyMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk <= any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 不等于any max 多个值的
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void notEqualAnyMaxSomeValueTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk != any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 不等于any max 1个值的
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void notEqualAnyMaxOneValueTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk != any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 等于all 最大值 多个值的
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void equalAllMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = all(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 等于all 最大值一个值的
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void equalAllMaxOneValueTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = all(select max(id) from " + studentTableName
                     + " )";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 大于all 最大值 多个值的
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void greatAllMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk > all(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 大于等于all 最大值 多个值的
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void greatEqualAllMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk >= all(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 小于all 最大值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void lessAllMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk < all(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 小于等于all 最大值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void lessEqualAllMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk <= all(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 不等于all 最大值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void notEqualAllMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk != all(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 等于all 一个值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void equalAllOneValueTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = all(select id from " + studentTableName
                     + " where id=1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 等于all 多个值
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void equalAllSomeValueTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = all(select id from " + studentTableName + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }
}
