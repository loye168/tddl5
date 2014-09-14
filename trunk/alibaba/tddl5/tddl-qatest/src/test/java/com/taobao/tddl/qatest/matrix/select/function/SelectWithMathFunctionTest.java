package com.taobao.tddl.qatest.matrix.select.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * 数据函数操作
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class SelectWithMathFunctionTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectWithMathFunctionTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void prepare() throws Exception {
        normaltblPrepare(0, 20);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    /**
     * min()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void minTest() throws Exception {
        String sql = "SELECT MIN(pk) as m FROM " + normaltblTableName;
        String[] columnParam = { "m" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "SELECT MIN(pk) as m FROM " + normaltblTableName + " where id>400";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * min() as min
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void minWithAliasTest() throws Exception {
        String sql = "SELECT MIN(pk) AS min FROM " + normaltblTableName;
        String[] columnParam = { "min" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * max()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void maxTest() throws Exception {
        String sql = "SELECT MAX(pk) FROM " + normaltblTableName;
        String[] columnParam = { "MAX(pk)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "SELECT MAX(pk) FROM " + normaltblTableName + " where id>400";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * min(),max()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @SuppressWarnings("unchecked")
    @Test
    public void maxMinTest() throws Exception {
        String sql = "SELECT MAX(pk),MIN(pk) FROM " + normaltblTableName;
        String[] columnParam = { "MAX(pk)", "MIN(pk)" };
        rc = null;
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * sum(pk)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void sumTest() throws Exception {
        String sql = "SELECT SUM(pk) FROM " + normaltblTableName;
        String[] columnParam = { "SUM(pk)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "SELECT SUM(pk) FROM " + normaltblTableName + " where id>400";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * sum(float)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void sumFloatTest() throws Exception {
        String sql = "SELECT SUM(floatCol) FROM " + normaltblTableName;
        String[] columnParam = { "SUM(floatCol)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    /**
     * Sum函数字段中为int或者long类型统一返回long类型
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void sumIntTest() throws Exception {
        String sql = "SELECT SUM(id) FROM " + normaltblTableName;
        String[] columnParam = { "SUM(id)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 统计多个总数
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void sumMutilTest() throws Exception {
        String sql = "SELECT SUM(id),sum(pk),sum(floatCol) FROM " + normaltblTableName;
        String[] columnParam = { "SUM(id)", "sum(pk)", "sum(floatCol)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * avg()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void avgLongTest() throws Exception {
        String sql = "SELECT AVG(pk) FROM " + normaltblTableName;
        String[] columnParam = { "AVG(PK)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "SELECT AVG(pk) FROM " + normaltblTableName + " where id >400";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * avg(int)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void avgIntTest() throws Exception {
        String sql = "SELECT AVG(id) FROM " + normaltblTableName;
        String[] columnParam = { "AVG(id)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * avg(float)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void avgFloatTest() throws Exception {
        String sql = "SELECT AVG(floatCol) FROM " + normaltblTableName;
        String[] columnParam = { "AVG(FLOATCOL)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * count(long)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void countTest() throws Exception {
        String sql = "SELECT COUNT(pk) FROM " + normaltblTableName;
        String[] columnParam = { "COUNT(PK)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * count(float)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void countNonPKTest() throws Exception {
        String sql = "SELECT COUNT(floatCol) FROM " + normaltblTableName;
        String[] columnParam = { "COUNT(FLOATCOL)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * count(*)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void countAllTest() throws Exception {
        String sql = "SELECT COUNT(*) FROM " + normaltblTableName;
        String[] columnParam = { "COUNT(*)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * count(1)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void count1Test() throws Exception {
        String sql = "SELECT COUNT(1) FROM " + normaltblTableName;
        String[] columnParam = { "COUNT(1)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * count(long) + where
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void countWithWhereTest() throws Exception {
        String sql = "SELECT COUNT(pk) FROM " + normaltblTableName + " where id>150";
        String[] columnParam = { "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * count(distinct)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void countWithDistinctTest() throws Exception {
        String sql = "SELECT COUNT(distinct name) FROM " + normaltblTableName;
        String[] columnParam = { "COUNT(distinct name)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * count(distinct) + groupby
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void countWithDistinctAndGroupByTest() throws Exception {
        String sql = "/* TDDL ALLOW_TEMPORARY_TABLE=True */ SELECT COUNT(DISTINCT ID) c FROM " + normaltblTableName
                     + " group by name";
        String[] columnParam = { "c" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * count(distinct col1,col2)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void countWithDistinctMutilCloumnTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "SELECT COUNT(distinct name,gmt_create) as d FROM " + normaltblTableName;
            String[] columnParam1 = { "d" };
            selectOrderAssert(sql, columnParam1, Collections.EMPTY_LIST);
        }
    }

    /**
     * round()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void roundTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select round(floatCol,2) as a from " + normaltblTableName;
            String[] columnParam = { "a" };
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

            sql = "select round(floatCol) from " + normaltblTableName + " where id >400";
            String[] columnParam1 = { "round(floatCol)" };
            selectContentSameAssert(sql, columnParam1, Collections.EMPTY_LIST);

            sql = "select round(id/pk,2) as a from " + normaltblTableName + " where name=?";
            List<Object> param = new ArrayList<Object>();
            param.add(name);
            String[] columnParam2 = { "a" };
            selectContentSameAssert(sql, columnParam2, param);
        }
    }

    /**
     * interval()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void intervalTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select interval(pk,id) as d FROM " + normaltblTableName;
            String[] columnParam1 = { "d" };
            selectContentSameAssert(sql, columnParam1, Collections.EMPTY_LIST);
        }
    }

    /**
     * sum() div sum()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void divTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select SUM(id) div sum(pk) as d FROM " + normaltblTableName;
            String[] columnParam1 = { "d" };
            selectContentSameAssert(sql, columnParam1, Collections.EMPTY_LIST);
        }
    }

    /**
     * sum() div sum()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void divisionTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select SUM(id) / sum(pk) as d FROM " + normaltblTableName;
            String[] columnParam1 = { "d" };
            selectContentSameAssert(sql, columnParam1, Collections.EMPTY_LIST);
        }
    }

    /**
     * sum() & sum()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void bitAndTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select SUM(id) & sum(pk) as d FROM " + normaltblTableName;
            String[] columnParam1 = { "d" };
            selectContentSameAssert(sql, columnParam1, Collections.EMPTY_LIST);
        }
    }

    /**
     * sum() | sum()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void bitOrTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select SUM(id) | sum(pk) as d FROM " + normaltblTableName;
            String[] columnParam1 = { "d" };
            selectContentSameAssert(sql, columnParam1, Collections.EMPTY_LIST);
        }
    }

    /**
     * sum() ^ sum()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void bitXorTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select SUM(id) ^ sum(pk) as d FROM " + normaltblTableName;
            String[] columnParam1 = { "d" };
            selectContentSameAssert(sql, columnParam1, Collections.EMPTY_LIST);
        }
    }

    /**
     * sum() >> 2
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void bitLShiftTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select SUM(id) >> 2 as d FROM " + normaltblTableName;
            String[] columnParam1 = { "d" };
            selectContentSameAssert(sql, columnParam1, Collections.EMPTY_LIST);
        }

    }

    /**
     * sum() << 2
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void bitRShiftTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select SUM(id) << 2 as d FROM " + normaltblTableName;
            String[] columnParam1 = { "d" };
            selectContentSameAssert(sql, columnParam1, Collections.EMPTY_LIST);
        }
    }

}
