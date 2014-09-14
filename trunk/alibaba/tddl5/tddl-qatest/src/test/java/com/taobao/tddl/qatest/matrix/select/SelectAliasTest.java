package com.taobao.tddl.qatest.matrix.select;

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
 * select别名
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class SelectAliasTest extends BaseMatrixTestCase {

    long pk = 1l;
    int  id = 1;

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblStudentTable(dbType));
    }

    public SelectAliasTest(String normaltblTableName, String studentTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
        BaseTestCase.studentTableName = studentTableName;
    }

    @Before
    public void prepareData() throws Exception {
        normaltblPrepare(0, MAX_DATA_SIZE);
        studentPrepare(0, MAX_DATA_SIZE);
    }

    /**
     * select 表别名
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasTableTest() throws Exception {
        String sql = "select * from  " + normaltblTableName + "  nor where nor.pk=?";
        sql = String.format("select * from  %s nor where nor.pk=?", normaltblTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        String[] columnParam = { "PK", "NAME", "ID" };
        assertAlias(sql, columnParam, "nor", param);
    }

    /**
     * select域常量别名
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasScalarTest() throws Exception {
        String sql = "select 1 as id from  " + normaltblTableName + " nor limit 1";
        String[] columnParam = { "ID" };
        assertAlias(sql, columnParam, "nor", null);
    }

    /**
     * select 表别名 where中用索引
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasTableTestWithIndexQuery() throws Exception {
        String sql = "select * from  " + normaltblTableName + "  nor where nor.id=?";
        List<Object> param = new ArrayList<Object>();
        param.add(id);
        String[] columnParam = { "PK", "NAME", "ID" };
        assertAlias(sql, columnParam, "nor", param);
    }

    /**
     * select 带as表别名
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasWithAsTableTest() throws Exception {
        String sql = "select * from  " + normaltblTableName + "  as nor where nor.pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);

        String[] columnParam = { "PK", "NAME", "ID" };
        assertAlias(sql, columnParam, "nor", param);
    }

    /**
     * select 列别名
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasFiledTest() throws Exception {
        String sql = "select name xingming ,id pid ,pk ppk from  " + normaltblTableName + "  where pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);

        String[] columnParam = { "ppk", "pid", "xingming" };
        assertAlias(sql, columnParam, "nor", param);
    }

    /**
     * select 带as列别名
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasFieldWithAsTest() throws Exception {
        String sql = "select name as xingming ,id as pid from  " + normaltblTableName + "  where pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        String[] columnParam = { "xingming", "pid" };
        assertAlias(sql, columnParam, "nor", param);
    }

    /**
     * select 表别名 列别名
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasFiledTableTest() throws Exception {
        String sql = "select name xingming ,id pid from  " + normaltblTableName + "  as nor where pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);

        String[] columnParam = { "xingming", "pid" };
        assertAlias(sql, columnParam, "nor", param);
    }

    /**
     * select 表别名 列别名上带as
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasFiledTableTest1() throws Exception {
        String sql = "select name as xingming ,id pid from  " + normaltblTableName + "  nor where pk=?";
        List<Object> param = new ArrayList<Object>();
        param.clear();
        param.add(pk);
        String[] columnParam = { "xingming", "pid" };
        assertAlias(sql, columnParam, "nor", param);
    }

    /**
     * select 表别名 join查询
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasTableWithJoinTest() throws Exception {
        String sql = "select n.name,s.name studentName,n.pk,s.id from  " + normaltblTableName + "  n , "
                     + studentTableName + "  s where n.pk=s.id";
        String[] columnParam = { "name", "studentName", "pk", "id" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select n.name,s.name studentName,n.pk,s.id from  " + normaltblTableName + "  as n , " + studentTableName
              + "  AS s where n.pk=s.id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * select 列别名 join查询
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasWithFieldJoinTest() throws Exception {
        String sql = "select  " + normaltblTableName + ".name name1, " + studentTableName + ".name, "
                     + normaltblTableName + ".pk pk1, " + studentTableName + ".id from  " + normaltblTableName + " , "
                     + studentTableName + " where  " + normaltblTableName + ".pk= " + studentTableName + ".id";
        String[] columnParam = { "name1", "name", "pk1", "id" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select  " + normaltblTableName + ".name as name1, " + studentTableName + ".name, " + normaltblTableName
              + ".pk as pk1, " + studentTableName + ".id from  " + normaltblTableName + " , " + studentTableName
              + " where  " + normaltblTableName + ".pk= " + studentTableName + ".id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * select 表别名 列别名 join查询
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasWithFieldTableJoinTest() throws Exception {
        String sql = "select n.name name1, " + studentTableName + ".name,n.pk pk1, " + studentTableName + ".id from  "
                     + normaltblTableName + "  n, " + studentTableName + " where n.pk= " + studentTableName + ".id";
        String[] columnParam = { "pk1", "id", "name1", "name" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select n.name as name1, " + studentTableName + ".name,n.pk pk1, " + studentTableName + ".id from  "
              + normaltblTableName + "  as n, " + studentTableName + " where n.pk= " + studentTableName + ".id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * select 列别名 groupby
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasWithGroupByTest() throws Exception {
        String sql = "select count(pk) ,name as n from  " + normaltblTableName + "   group by n";
        String[] columnParam = { "count(pk)", "n" };
        try {
            selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
        } catch (Exception ex) {
            System.out.print(ex);
        }
    }

    /**
     * select 列别名 orderby
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasWithOrderByTest() throws Exception {
        String sql = "select pk as p ,name as n from  " + normaltblTableName + "   order by p asc";
        String[] columnParam = { "p", "n" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * select count函数别名
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @SuppressWarnings("unchecked")
    @Test
    public void aliasWithFuncByTest() throws Exception {
        String sql = "select count(pk) as con from  " + normaltblTableName + " ";
        String[] columnParam = { "con" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * select sum函数别名
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void aliasWithFuncWithGroupBy() throws Exception {
        String sql = "select id ,sum(pk) as p from " + normaltblTableName + " as a group by id";
        String[] columnParam = { "id", "p" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * select 子查询别名
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    // TODO(目前子查询不支持)
    @Test
    public void aliasWithSubQueryTest() throws Exception {

    }

}
