package com.taobao.tddl.qatest.matrix.select;

import java.util.ArrayList;
import java.util.Arrays;
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
 * 带条件的选择查询
 * 
 * @author zhuoxue
 * @since 5.0.1
 */

@RunWith(EclipseParameterized.class)
public class SelectWithFilterTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectWithFilterTest(String normaltblTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
    }

    @Before
    public void prepareDate() throws Exception {
        normaltblPrepare(0, 20);
    }

    /**
     * 大于
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void greaterTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk)>? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(5L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * 大于等于
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void greaterEqualTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk)>=? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * 小于
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void lessTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk)<? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(5L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * 小于等于
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void lessEqualTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk)<=? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(9L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * 等于
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void equalTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk)=? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * in
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void inTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk) in (?) order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * 不等于
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void notEqualTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk) != ? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * having中带and
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void andTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk) != ? and count(pk) < ? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        param.add(11L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * group or
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void groupOrTest() throws Exception {
        String sql = "select name,count(pk) from "
                     + normaltblTableName
                     + " where (pk > 2 or pk in (1,3,5,7)) AND name is not null group by name having count(pk) in (?) order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * having中带or
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void orTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk) != ? or count(pk) < ? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        param.add(11L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * having中带and、or
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void andOrTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having (count(pk) != ? or count(pk) < ?) and count(pk)>? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        param.add(11L);
        param.add(5L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * having中带常量
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void constantTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName + " group by name having 1 order by name ";
        List<Object> param = new ArrayList<Object>();

        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * having中带true常量
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void constantTest2() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName + " group by name having true order by name ";
        List<Object> param = new ArrayList<Object>();

        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * having中带‘true’常量
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void constantTest3() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName + " group by name having 'true' order by name ";
        List<Object> param = new ArrayList<Object>();

        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * having中带 常量and常量
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void constantAndTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having 1 and 2 order by name ";
        List<Object> param = new ArrayList<Object>();

        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * having中带 常量or常量
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void constantOrTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName + " group by name having 1 or 2 order by name ";
        List<Object> param = new ArrayList<Object>();

        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

}
