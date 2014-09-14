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
 * select缓存测试
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class SelectCacheTest extends BaseMatrixTestCase {

    String[] columnParam = { "PK", "NAME", "ID", "gmt_create", "GMT_TIMESTAMP", "GMT_DATETIME", "floatCol" };

    @Parameters(name = "{index}:table1={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectCacheTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void prepareDate() throws Exception {
        normaltblPrepare(0, 20);
    }

    /**
     * 给id多组不同的值验证
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectWhereTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            for (int i = 0; i < 4; i++) {
                String sql = "select * from " + normaltblTableName + " where id=?";
                List<Object> param = new ArrayList<Object>();
                param.add(i);

                selectContentSameAssert(sql, columnParam, param);

                sql = "select * from " + normaltblTableName + " where id=? and name =?";
                param.clear();
                param.add(i);
                param.add(name);
                selectContentSameAssert(sql, columnParam, param);

                sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select * from " + normaltblTableName
                      + " where id=? or pk =?";
                param.clear();
                param.add(i);
                param.add(Long.parseLong(i + 1 + ""));
                selectContentSameAssert(sql, columnParam, param);
            }
        }
    }

    /**
     * 表别名查询
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectAliasTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            for (long i = 0; i < 4; i++) {
                String sql = "select * from  " + normaltblTableName + "  nor where nor.pk=?";
                List<Object> param = new ArrayList<Object>();
                param.add(i);
                selectContentSameAssert(sql, columnParam, param);
            }
        }
    }

    /**
     * order by
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectOrderTest() throws Exception {
        for (long i = 0; i < 4; i++) {
            String sql = "select * from " + normaltblTableName + " where pk= ? order by name";
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            selectOrderAssertNotKeyCloumn(sql, columnParam, param, "name");

            sql = "select * from " + normaltblTableName + " where pk= ? order by name desc";
            selectOrderAssertNotKeyCloumn(sql, columnParam, param, "name");
        }
    }

    /**
     * limit
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectLimitTest() throws Exception {
        String[] stringName = { name, newName, name1 };
        for (int i = 0; i < stringName.length; i++) {
            String sql = "select * from " + normaltblTableName + " where name= ? order by id limit 2";
            List<Object> param = new ArrayList<Object>();
            param.add(stringName[i]);
            selectOrderAssertNotKeyCloumn(sql, columnParam, param, "id");

            sql = "select * from " + normaltblTableName + " where name= ? order by id desc limit 2";
            selectOrderAssertNotKeyCloumn(sql, columnParam, param, "id");
        }
    }

    /**
     * groupby orderby
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectLGroupTest() throws Exception {
        String[] stringName = { name, newName, name1 };
        String[] columnParam = { "gmt_timestamp", "count(pk)" };
        for (int i = 0; i < stringName.length; i++) {
            String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select gmt_timestamp,count(pk) from "
                         + normaltblTableName + " where name=? group by gmt_timestamp  order by count(pk)";
            List<Object> param = new ArrayList<Object>();
            param.add(stringName[i]);
            selectContentSameAssert(sql, columnParam, param);
        }
    }

    /**
     * where后为比较操作符
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectOperatorTest() throws Exception {
        for (int i = 0; i < 4; i++) {
            String sql = "select * from " + normaltblTableName + " where id>?";
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            selectContentSameAssert(sql, columnParam, param);

            sql = "select * from " + normaltblTableName + " where id >=? and id<?";
            param.clear();
            param.add(i);
            param.add(i + 10);
            selectContentSameAssert(sql, columnParam, param);
        }
    }

    /**
     * where后为between
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectBetweenTest() throws Exception {
        for (int i = 0; i < 4; i++) {
            String sql = "select * from " + normaltblTableName + " where id between ? and ?";
            List<Object> param = new ArrayList<Object>();
            param.add(i - 3);
            param.add(i + 4);
            selectContentSameAssert(sql, columnParam, param);

            sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select * from " + normaltblTableName
                  + " where id between ? and ? order by name";
            param.clear();
            param.add(i - 3);
            param.add(i + 4);
            selectContentSameAssert(sql, columnParam, param);
        }
    }

    /**
     * where后为like
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void selectLikeTest() throws Exception {
        String[] stringName = { name, newName, name1 };
        for (int i = 0; i < stringName.length; i++) {
            String sql = "select * from " + normaltblTableName + " where name like ?";
            List<Object> param = new ArrayList<Object>();
            param.add(stringName[i]);
            selectContentSameAssert(sql, columnParam, param);

            sql = "select * from " + normaltblTableName + " where name like ? and id>" + 3;
            selectContentSameAssert(sql, columnParam, param);
        }
    }

    /**
     * insert
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void insertTest() throws Exception {
        tddlUpdateData("delete from  " + normaltblTableName, null);
        mysqlUpdateData("delete from  " + normaltblTableName, null);
        for (long i = 0; i < 4; i++) {
            String sql = "insert into " + normaltblTableName
                         + " (pk,floatCol,gmt_create,gmt_timestamp,gmt_datetime)values(?,?,?,?,?)";
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(fl);
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            execute(sql, param);
            sql = "select * from " + normaltblTableName + " where pk= ?";
            param.clear();
            param.add(i);
            String[] columnParam = { "PK", "GMT_CREATE", "GMT_TIMESTAMP", "GMT_DATETIME", "FLOATCOL" };
            selectOrderAssert(sql, columnParam, param);
        }
    }

    /**
     * replace
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceTest() throws Exception {
        tddlUpdateData("delete from  " + normaltblTableName, null);
        mysqlUpdateData("delete from  " + normaltblTableName, null);
        for (long i = 0; i < 4; i++) {
            String sql = "replace into " + normaltblTableName
                         + " (pk,floatCol,gmt_create,gmt_timestamp,gmt_datetime)values(?,?,?,?,?)";
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(fl);
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            execute(sql, param);
            sql = "select * from " + normaltblTableName + " where pk= ?";
            param.clear();
            param.add(i);
            String[] columnParam = { "PK", "GMT_CREATE", "FLOATCOL", "GMT_DATETIME", "FLOATCOL" };
            selectOrderAssert(sql, columnParam, param);
        }
    }

    /**
     * update
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            for (long i = 0; i < 4; i++) {
                String sql = "UPDATE " + normaltblTableName
                             + " SET id=?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=? WHERE pk=?";
                List<Object> param = new ArrayList<Object>();
                param.add(rand.nextInt());
                param.add(gmt);
                param.add(gmt);
                param.add(gmt);
                param.add("new_name" + rand.nextInt());
                param.add(fl);
                param.add(i);
                executeCountAssert(sql, param);

                sql = "SELECT * FROM " + normaltblTableName + " WHERE pk=?";
                param.clear();
                param.add(i);
                String[] columnParam = { "ID", "GMT_CREATE", "GMT_DATETIME", "FLOATCOL", "NAME", "FLOATCOL" };
                selectOrderAssert(sql, columnParam, param);
            }
        }
    }

    /**
     * 更新自增测试
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void updateIncrementTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            for (long i = 0; i < 4; i++) {
                String sql = "UPDATE " + normaltblTableName
                             + " SET id=id+?,gmt_create=?,gmt_timestamp=?,gmt_datetime=?,name=?,floatCol=? WHERE pk=?";
                List<Object> param = new ArrayList<Object>();
                param.add(rand.nextInt());
                param.add(gmt);
                param.add(gmt);
                param.add(gmt);
                param.add("new_name" + rand.nextInt());
                param.add(fl);
                param.add(i);
                executeCountAssert(sql, param);

                sql = "SELECT * FROM " + normaltblTableName + " WHERE pk=?";
                param.clear();
                param.add(i);
                String[] columnParam = { "ID", "GMT_CREATE", "GMT_DATETIME", "FLOATCOL", "NAME", "FLOATCOL" };
                selectOrderAssert(sql, columnParam, param);
            }
        }
    }

    /**
     * delete
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void deleteTest() throws Exception {
        for (long i = 0; i < 4; i++) {
            String sql = "DELETE FROM " + normaltblTableName + " WHERE pk = ?";
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            executeCountAssert(sql, Arrays.asList(new Object[] { i }));
        }
    }

}
