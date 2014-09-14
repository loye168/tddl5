package com.taobao.tddl.qatest.matrix.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * LocalServerSelectTest
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class SelectTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.studentTable(dbType));
    }

    public SelectTest(String studentTableName){
        BaseTestCase.studentTableName = studentTableName;
    }

    @Before
    public void initData() throws Exception {
        tddlUpdateData("delete from  " + studentTableName, null);
        mysqlUpdateData("delete from  " + studentTableName, null);
    }

    /**
     * 查询所有列
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectAllFieldTest() throws Exception {
        tddlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        mysqlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        String sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;
        String[] columnParam = { "NAME", "SCHOOL" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 查询所有列 带count(*)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectAllFieldWithFuncTest() throws Exception {
        tddlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        mysqlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        String sql = "select *,count(*) from " + studentTableName + " where id=" + RANDOM_ID;
        String[] columnParam = { "NAME", "SCHOOL", "id", "count(*)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 查询部分列
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectSomeFieldTest() throws Exception {
        tddlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        mysqlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        String sql = "select id,name from " + studentTableName + " where id=" + RANDOM_ID;
        String[] columnParam = { "NAME", "ID" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select id,name from " + studentTableName + " where name= ?";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * 查询时条件中带 quotation
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectWithQuotationTest() throws Exception {
        String name = "as'sdfd's";
        tddlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        mysqlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        String[] columnParam = { "NAME", "ID" };

        String sql = "select id,name from " + studentTableName + " where name= ?";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        selectOrderAssert(sql, columnParam, param);

        sql = "select id,name from " + studentTableName + " where name= 'as\\'sdfd\\'s'";
        selectOrderAssert(sql, columnParam, null);
    }

    /**
     * 查询时条件中带不存在的时间
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectWithNotExistDateTest() throws Exception {
        tddlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        mysqlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        String sql = "select * from " + studentTableName + " where id=" + RANDOM_ID + 1;

        selectConutAssert(sql, Collections.EMPTY_LIST);
    }

    /**
     * 查询时条件中带 不存在的列
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectWithNotExistFileTest() throws Exception {
        tddlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        String sql = "select * from " + studentTableName + " where pk=" + RANDOM_ID;
        try {
            rc = tddlQueryData(sql, null);
            rc.next();
            Assert.fail();
        } catch (Exception ex) {
            // Assert.assertTrue(ex.getMessage().contains("column: PK is not existed in"));
        }
    }

    /**
     * 不存在的表 查询
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectWithNotExistTableTest() throws Exception {
        String sql = "select * from stu where pk=" + RANDOM_ID;
        try {
            rc = tddlQueryData(sql, null);
            rc.next();
            Assert.fail();
        } catch (Exception ex) {
            // Assert.assertTrue(ex.getMessage().contains("STU is not found"));
        }
    }
}
