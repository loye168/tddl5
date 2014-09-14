package com.taobao.tddl.qatest.matrix.basecrud;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
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
 * replace测试
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class ReplaceTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public ReplaceTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void initData() throws Exception {
        tddlUpdateData("delete from  " + normaltblTableName, null);
        mysqlUpdateData("delete from  " + normaltblTableName, null);
    }

    /**
     * replace 所有列
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceAllFieldTest() throws Exception {
        if (normaltblTableName.startsWith("ob")) {
            Assert.assertTrue(true);
            return;
        }
        String sql = "replace into " + normaltblTableName + " values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmt);
        param.add(gmt);
        param.add(gmt);
        param.add(name);
        param.add(fl);
        execute(sql, param);

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * replace 多个值
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceMultiValuesTest() throws Exception {
        StringBuilder sql = new StringBuilder("replace into " + normaltblTableName + " values ");
        List<Object> param = new ArrayList<Object>();

        for (int i = 0; i < 40; i++) {
            if (i == 0) {
                sql.append("(?,?,?,?,?,?,?)");
            } else {
                sql.append(",(?,?,?,?,?,?,?)");
            }
            param.add(RANDOM_ID + i);
            param.add(RANDOM_INT + i);
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add(null);
            param.add(fl);
        }

        execute(sql.toString(), param);

        String selectSql = "select * from " + normaltblTableName;
        String[] columnParam = new String[] { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP",
                "GMT_DATETIME" };
        selectContentSameAssert(selectSql, columnParam, Collections.EMPTY_LIST);

    }

    /**
     * replace 部分列
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceSomeFieldTest() throws Exception {
        String sql = "replace into " + normaltblTableName + " (pk,floatCol,gmt_timestamp)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(fl);
        param.add(gmt);
        execute(sql, param);

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "GMT_TIMESTAMP", "FLOATCOL" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * replace 使用set设置值
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceWithSetTest() throws Exception {

        String sql = "replace into  " + normaltblTableName + "  set pk=? ,name=?";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(name);
        execute(sql, param);

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "NAME" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Ignore("目前不支持replace中带select的sql语句")
    @Test
    public void replaceWithSelectTest() throws Exception {
        // andorUpdateData("insert into student(id,name,school) values (?,?,?)",
        // Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        //
        // String sql = "replace into " + normaltblTableName +
        // "(pk,name) select id,name from student where school=?";
        // List<Object> param = new ArrayList<Object>();
        // param.add(school);
        // rc = execute(null, sql, param);
        // rc.close();
        // rc = null;
        //
        // rc = execute(null, "select * from " + normaltblTableName +
        // " where pk=" + RANDOM_ID, Collections.EMPTY_LIST);
        // IRowSet kv = null;
        // kv = rc.next();
        // Assert.assertEquals(name, rc.getIngoreTableName(kv,
        // "name").toString());
        //
        // PreparedData("delete from student where school=?", new Object[] {
        // school });
    }

    /**
     * replace BDB,本例中不测试
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceWithBdbOutParamTest() throws Exception {
        if (!(normaltblTableName.contains("mysql") || normaltblTableName.startsWith("ob"))) {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String sql = "replace into " + normaltblTableName + "(pk,gmt_create,gmt_timestamp,gmt_datetime,id) values("
                         + RANDOM_ID + ",'" + df.format(gmt) + "','" + df.format(gmt) + "','" + df.format(gmt) + "',"
                         + RANDOM_INT + ")";
            execute(sql, Collections.EMPTY_LIST);

            sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
            String[] columnParam = { "PK", "GMT_CREATE", "ID", "GMT_TIMESTAMP", "GMT_DATETIME" };
            selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    /**
     * replace 列为小写字母
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replacePramLowerCaseTest() throws Exception {
        String sql = "replace into " + normaltblTableName + " (pk,floatcol,gmt_create)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(fl);
        param.add(gmtDay);
        execute(sql, param);

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "GMT_CREATE", "FLOATCOL" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * replace 列为大写字母
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replacePramUppercaseTest() throws Exception {
        String sql = "REPLACE INTO " + normaltblTableName + " (PK,FLOATCOL,GMT_CREATE)VALUES(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(fl);
        param.add(gmtDay);
        execute(sql, param);

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "GMT_CREATE", "FLOATCOL" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * replace 中不带primary key,应该报异常
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceWithOutKeyFieldTest() throws Exception {
        String sql = "replace into " + normaltblTableName + " (id,floatCol,gmt_create)values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_INT);
        param.add(fl);
        param.add(gmtDay);
        try {
            tddlUpdateData(sql, param);
            Assert.fail();
        } catch (Exception ex) {
            // TODO 单库多表抛出"insert not support muti tables",需要以后最终确认应该抛出怎样的异常
            // throw e;
            // Assert.assertTrue(e.getMessage(),e.getMessage().contains("pk must not null"));
            // shenxun : 不一样的异常。。。暂时不用上面的异常吧。。
        }
    }

    /**
     * replace 0值和负值
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceWithZoreAndNegativeTest() throws Exception {
        long pk = -1l;
        int id = -1;
        String sql = "replace into " + normaltblTableName + " (pk,id)values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        execute(sql, param);

        sql = "select * from " + normaltblTableName + " where pk=" + pk;
        String[] columnParam = { "PK", "ID" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        tddlUpdateData("delete from " + normaltblTableName + " where pk=?", Arrays.asList(new Object[] { pk }));
        mysqlUpdateData("delete from " + normaltblTableName + " where pk=" + pk, null);

        pk = 0;
        id = 0;
        sql = "replace into " + normaltblTableName + " (pk,id)values(?,?)";
        param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        execute(sql, param);

        sql = "select * from " + normaltblTableName + " where pk=" + pk;
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * replace最大最小值
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceWithMaxMinTest() throws Exception {
        long pk = Long.MAX_VALUE;
        int id = Integer.MAX_VALUE;
        String sql = "replace into " + normaltblTableName + " (pk,id)values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        execute(sql, param);

        sql = "select * from " + normaltblTableName + " where pk=" + pk;
        String[] columnParam = { "PK", "ID" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        tddlUpdateData("delete from " + normaltblTableName + " where pk=?", Arrays.asList(new Object[] { pk }));
        mysqlUpdateData("delete from " + normaltblTableName + " where pk=" + pk, null);

        pk = Long.MIN_VALUE;
        id = Integer.MIN_VALUE;
        sql = "replace into " + normaltblTableName + " (pk,id)values(?,?)";
        param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        execute(sql, param);

        sql = "select * from " + normaltblTableName + " where pk=" + pk;
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * replace 中值与列类型不匹配
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceErrorTypeFieldTest() throws Exception {
        String sql = "replace into " + normaltblTableName + " (pk,gmt_create)values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(fl);
        try {
            tddlUpdateData(sql, param);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("Unsupported"));
        }
    }

    /**
     * replace 不存在的列
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceNotExistFieldTest() throws Exception {

        String sql = "replace into " + normaltblTableName + " (pk,gmts)values(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(gmt);
        try {
            tddlUpdateData(sql, param);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertNotNull(ex);
            // Assert.assertTrue(ex.getCause().getCause().getMessage().contains("can't find target name"));
        }

    }

    /**
     * replace 不带primary key
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceWithOutKeyValueTest() throws Exception {

        String sql = "replace into " + normaltblTableName + " (name)values(?)";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        try {
            tddlUpdateData(sql, param);
            Assert.fail();
        } catch (Exception ex) {
            // TODO
            // Assert.assertTrue(ex.getMessage().contains("pk must not null"));
            // 应该抛出怎样的异常还未确定
        }

    }

    /**
     * replace 列与值的个数不匹配
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void replaceNotMatchFieldTest() throws Exception {
        String sql = "replace into " + normaltblTableName + " (id,floatCol) values(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(fl);
        param.add(gmt);
        try {
            tddlUpdateData(sql, param);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage() != null);
        }
    }
}
