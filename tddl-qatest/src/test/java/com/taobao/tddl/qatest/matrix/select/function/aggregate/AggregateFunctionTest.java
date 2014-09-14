package com.taobao.tddl.qatest.matrix.select.function.aggregate;

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;

/**
 * 不能下推的聚合函数测试
 * 
 * @author mengshi.sunmengshi 2014年4月16日 上午11:43:04
 * @since 5.1.0
 */
public class AggregateFunctionTest extends BaseMatrixTestCase {

    public AggregateFunctionTest(){
        BaseTestCase.normaltblTableName = "_tddl_";
    }

    @Before
    public void prepare() throws Exception {
        demoRepoPrepare(0, 20);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    /**
     * count(*)
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void countTest() throws Exception {
        String sql = "select count(*) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    /**
     * sum(id)
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void sumTest() throws Exception {
        String sql = "select sum(id) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    /**
     * sum(id)/count(id)
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void operationTest() throws Exception {
        String sql = "select sum(id)/count(id) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    /**
     * sum(id+1)/count(id)
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void operationTest1() throws Exception {
        String sql = "select sum(id+1)/count(id) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    /**
     * sum(id+1)/count(id) as a,count(id)+1
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void operationTest2() throws Exception {
        String sql = "select sum(id+1)/count(id) as a,count(id)+1 as b  from " + normaltblTableName;
        String[] columnParam = { "a", "b" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    /**
     * sum(id+1)/count(id) as a,count(id)+1
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void operationTest3() throws Exception {
        String sql = "select sum(id+1)/count(id) as a,count(id)+1 as b  from " + normaltblTableName;
        String[] columnParam = { "a", "b" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    /**
     * count(id)+1 as b ,count(id)
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void operationTest4() throws Exception {
        String sql = "select count(id)+1 as b ,count(id) as c from " + normaltblTableName;
        String[] columnParam = { "b", "c" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    /**
     * count(distinct id)
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void distinctTest() throws Exception {
        String sql = "select count(distinct id) b,count(id) c from " + normaltblTableName;
        String[] columnParam = { "b", "c" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    /**
     * order by id asc
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void orderByAscTest() throws Exception {
        String sql = "select id,name from _tddl_ order by id asc";
        String[] columnParam = { "id", "name" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    /**
     * order by id desc
     * 
     * @author mengshi
     * @since 5.1.0
     */
    @Test
    public void orderByDescTest1() throws Exception {
        String sql = "select id,name from _tddl_ order by id desc";
        String[] columnParam = { "id", "name" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

}
