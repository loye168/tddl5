package com.taobao.tddl.qatest.matrix.select;

import java.util.ArrayList;
import java.util.Arrays;
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
 * 带条件的选择查询
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class SelectWithOperatorTest extends BaseMatrixTestCase {

    String[] columnParam = { "PK", "NAME", "ID" };

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectWithOperatorTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
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
        String sql = "select * from " + normaltblTableName + " where pk>? order by pk";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(0 + ""));
        selectOrderAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where pk>=? order by pk";
        param.clear();
        param.add(Long.parseLong(0 + ""));
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
        String sql = "select * from " + normaltblTableName + " where pk < ?";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(MAX_DATA_SIZE + ""));
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where pk <= ?";
        param.clear();
        param.add(Long.parseLong(MAX_DATA_SIZE - 1 + ""));
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where ? <= pk";
        param.clear();
        param.add(Long.parseLong(10 + ""));
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * 小于 and 大于
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void lessAndGreatTest() throws Exception {
        int start = 5;
        int end = 15;

        String sql = "select * from " + normaltblTableName + " where pk >=? and pk< ?";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(start + ""));
        param.add(Long.parseLong(end + ""));
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where id >=? and id< ?";
        param.clear();
        param.add(start);
        param.add(end);
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * String类型大于小于比较
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void lessAndGreatWithStringTest() throws Exception {
        int start = 5;
        String sql = "select * from " + normaltblTableName + " where pk >=? and name>?";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(start + ""));
        param.add(name);
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where name<?";
        param.clear();
        param.add(name);
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where name >?";
        param.clear();
        param.add(name);
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * 多次比较
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void mutilCompareTest() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        float fl = 0.15f;
        String sql = "select * from " + normaltblTableName + " where pk>=? and pk>? and pk <=? and pk<?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        String[] columnParam = { "PK", "NAME", "ID", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where pk>=? and pk>? and name like ? or gmt_timestamp >?";
        param.clear();
        param.add(start1);
        param.add(start2);
        param.add(name);
        param.add(gmtNext);
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where pk>=? and pk>? and name like ? or gmt_timestamp >?";
        param.clear();
        param.add(start1);
        param.add(start2);
        param.add(name);
        param.add(gmtNext);
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where pk>? or (id<? and name like ? and gmt_timestamp= ?)";
        param.clear();
        param.add(start1);
        param.add(1500);
        param.add(name);
        param.add(gmt);
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName
              + " where pk<=? and id>?  or name like ? and gmt_timestamp =? or floatCol=?";
        param.clear();
        param.add(start1);
        param.add(516);
        param.add(name);
        param.add(gmt);
        param.add(fl);
        selectConutAssert(sql, param);
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName
              + " where pk<=? and id>?  or name like ? and gmt_datetime =? or floatCol=?";
        param.clear();
        param.add(start1);
        param.add(516);
        param.add(name);
        param.add(gmt);
        param.add(fl);
        selectConutAssert(sql, param);
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * 小于大于 没数据的情况
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void lessAndGreatNoDataTest() throws Exception {
        long start = 5;
        long end = 15;
        String sql = "select * from " + normaltblTableName + " where pk<? and pk>?";
        List<Object> param = new ArrayList<Object>();
        param.add(start);
        param.add(end);
        {
            rs = mysqlQueryData(sql, param);
            rc = tddlQueryData(sql, param);
            Assert.assertEquals(resultsSize(rs), resultsSize(rc));
        }
    }

    /**
     * 列的比较
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void cloumnCompareTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk > id";
        selectContentSameAssert(sql, columnParam, null);
    }

    /**
     * 不等于
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void notEqualsTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk <> ?";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(0 + ""));

        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where pk != ?";
        param.clear();
        param.add(Long.parseLong(0 + ""));
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * 不等于 加in
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void notEqualsWithInTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk in(?,?,?) and id !=? and id !=? and name =?";
        List<Object> param = new ArrayList<Object>();
        param.add(1l);
        param.add(2l);
        param.add(3l);
        param.add(100);
        param.add(700);
        param.add(name);
        selectContentSameAssert(sql, columnParam, param);
    }

    @Test
    @Ignore
    // 不支持这个符号
    public void equalsTest() throws Exception {
        tddlUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, null }));
        String sql = "select * from " + normaltblTableName + " where name <=> ?";
        List<Object> param = new ArrayList<Object>();
        param.add(null);
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * 操作符：位操作与 &
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void bitwiseAndTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select count(*) from " + normaltblTableName
                         + " where pk in (?,?,?) and name =? and id & ? = ?";
            String[] columnParam = { "count(*)" };
            List<Object> param = new ArrayList<Object>();
            param.add(1);
            param.add(2);
            param.add(3);
            param.add(name);
            param.add(300);
            param.add(300);
            selectContentSameAssert(sql, columnParam, param);
        }
    }

    /**
     * 操作符：位操作与 & 和like操作
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void bitwiseLikeTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select count(*) from " + normaltblTableName
                         + " where pk in (?,?,?) and name like ? and id & ? = ?";
            String[] columnParam = { "count(*)" };
            List<Object> param = new ArrayList<Object>();
            param.add(1);
            param.add(2);
            param.add(3);
            param.add(name);
            param.add(300);
            param.add(300);
            selectContentSameAssert(sql, columnParam, param);
        }
    }

    /**
     * 操作符：位操作符 ^
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void xorTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select count(*) from " + normaltblTableName + " where pk ^ id > ?";
            String[] columnParam = { "count(*)" };
            List<Object> param = new ArrayList<Object>();
            param.add(100);
            selectContentSameAssert(sql, columnParam, param);

            sql = "select count(*) from " + normaltblTableName + " where pk xor id > ?";
            selectContentSameAssert(sql, columnParam, param);
        }
    }

    /**
     * 带有乘法操作
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void multiplicationTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where id > pk * floatCol";
        selectContentSameAssert(sql, columnParam, null);
    }

    /**
     * 操作符：%（模）运算
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void dieTest() throws Exception {
        String sql = "select * from " + normaltblTableName + "  where id % ? = ? and name =? ";
        List<Object> param = new ArrayList<Object>();
        param.add(100);
        param.add(0);
        param.add(name);
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * 除法操作：/
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void divisionTest() throws Exception {
        String sql = "select sum(id)/sum(pk)  as avg from " + normaltblTableName + "  where name =? ";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        String[] columnParam = { "avg" };
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * 复杂计算
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void complicateCalcuationTest() throws Exception {
        String sql = "select id/(pk+1)*floatCol as c from " + normaltblTableName + " where name=?";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        String[] columParam = { "c" };
        selectContentSameAssert(sql, columParam, param);
    }

    /**
     * 数学常量计算
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void constantCalcuationTest() throws Exception {
        String sql = "select 20*2 a ,id from " + normaltblTableName + " where name=?";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        String[] columParam = { "a", "id" };
        selectContentSameAssert(sql, columParam, param);
    }

    /**
     * 数学运算没有加别名
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void calcuationTest() throws Exception {
        String sql = "select 20*2 a ,id from " + normaltblTableName + " where name=?";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        String[] columParam = { "a", "id" };
        selectContentSameAssert(sql, columParam, param);
    }

    /**
     * where条件中有带算术运算的比较
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void calcuationWhereTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where id>=?+?";
        List<Object> param = new ArrayList<Object>();
        param.add(100);
        param.add(200);
        selectContentSameAssert(sql, columnParam, param);
        sql = "select * from " + normaltblTableName + " where ?+?!=id";
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * &&操作
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void and() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name like ? && pk > ? ";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        param.add(500);
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * case when操作
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void caseWhen() throws Exception {
        {
            String sql = "select case pk when 500 then 600 else 700 end pk1 from " + normaltblTableName;
            List<Object> param = new ArrayList<Object>();

            String[] columnParam = { "PK1" };
            selectContentSameAssert(sql, columnParam, param);
        }

        {
            String sql = "select case pk when 500 then 600 when 1 then 3 else 700 end pk1 from " + normaltblTableName;
            List<Object> param = new ArrayList<Object>();

            String[] columnParam = { "PK1" };
            selectContentSameAssert(sql, columnParam, param);
        }

        {
            String sql = "select case pk when 500 then 600 when 1 then 3 end pk1 from " + normaltblTableName;
            List<Object> param = new ArrayList<Object>();

            String[] columnParam = { "PK1" };
            selectContentSameAssert(sql, columnParam, param);
        }

        {
            String sql = "select case  when pk=500 then 600 else 700 end pk1 from " + normaltblTableName;
            List<Object> param = new ArrayList<Object>();

            String[] columnParam = { "PK1" };
            selectContentSameAssert(sql, columnParam, param);
        }

        {
            String sql = "select case  when pk=500 then 600 when pk>700 then 900 else 700 end pk1 from "
                         + normaltblTableName;
            List<Object> param = new ArrayList<Object>();

            String[] columnParam = { "PK1" };
            selectContentSameAssert(sql, columnParam, param);
        }

        {
            String sql = "select case  when pk=500 then 600 when pk>700 then 900 end pk1 from " + normaltblTableName;
            List<Object> param = new ArrayList<Object>();

            String[] columnParam = { "PK1" };
            selectContentSameAssert(sql, columnParam, param);
        }

        {
            String sql = "select case  when pk=500 then 600 when pk>700 then 900 else null end pk1 from "
                         + normaltblTableName;
            List<Object> param = new ArrayList<Object>();

            String[] columnParam = { "PK1" };
            selectContentSameAssert(sql, columnParam, param);
        }
    }
}
