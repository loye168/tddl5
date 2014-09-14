package com.taobao.tddl.qatest.matrix.hint;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;

/**
 * 条件hint
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
public class ConditionHintTest extends BaseMatrixTestCase {

    private JdbcTemplate jdbcTemplate;
    private Date         time   = new Date();
    private String       prefix = "/*+TDDL";

    public ConditionHintTest(){
        BaseTestCase.normaltblTableName = "mysql_normaltbl_oneGroup_oneAtom";
        jdbcTemplate = new JdbcTemplate(tddlDatasource);
    }

    @Before
    public void initData() throws Exception {
        tddlUpdateData("delete from mysql_normaltbl_oneGroup_oneAtom", null);
        tddlUpdateData("delete from mysql_normaltbl_onegroup_mutilatom", null);

        if (isTddlServer()) {
            prefix = "/!+TDDL";
        }
    }

    /**
     * 简单等值条件
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_简单等值条件() throws Exception {
        String sql = prefix
                     + "({'type':'condition','vtab':'mysql_normaltbl_onegroup_mutilatom','params':[{'expr':['pk=1:int']}]})*/";
        sql += "insert into mysql_normaltbl_onegroup_mutilatom values(?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        tddlUpdateData(sql, param);

        // 用直连库进行查询
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_2','vtab':'mysql_normaltbl_onegroup_mutilatom','realtabs':['mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 继续用规则hint查询
        sql = prefix
              + "({'type':'condition','vtab':'mysql_normaltbl_onegroup_mutilatom','params':[{'expr':['pk=1:int']}]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 执行删除
        sql = prefix
              + "({'type':'condition','vtab':'mysql_normaltbl_onegroup_mutilatom','params':[{'expr':['pk=1:int']}]})*/";
        sql += "delete from mysql_normaltbl_onegroup_mutilatom where pk = " + RANDOM_ID;
        tddlUpdateData(sql, null);

        // 删除完之后，应该查不到
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_2','vtab':'mysql_normaltbl_onegroup_mutilatom','realtabs':['mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        List list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());
    }

    /**
     * 简单等值条件 指定groupIndex
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_简单等值条件_指定groupIndex() throws Exception {
        String sql = prefix
                     + "({'type':'condition','vtab':'mysql_normaltbl_onegroup_mutilatom','params':[{'expr':['pk=1:int']}]})*/";
        sql += prefix + "_GROUP({groupIndex:0})*/";
        sql += "insert into mysql_normaltbl_onegroup_mutilatom values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        tddlUpdateData(sql, param);

        // 用直连库进行查询
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_2','vtab':'mysql_normaltbl_onegroup_mutilatom','realtabs':['mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += prefix + "_GROUP({groupIndex:0})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 继续用规则hint查询
        sql = prefix
              + "({'type':'condition','vtab':'mysql_normaltbl_onegroup_mutilatom','params':[{'expr':['pk=1:int']}]})*/";
        sql += prefix + "_GROUP({groupIndex:0})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 执行删除
        sql = prefix
              + "({'type':'condition','vtab':'mysql_normaltbl_onegroup_mutilatom','params':[{'expr':['pk=1:int']}]})*/";
        sql += prefix + "_GROUP({groupIndex:0})*/";
        sql += "delete from mysql_normaltbl_onegroup_mutilatom where pk = " + RANDOM_ID;
        tddlUpdateData(sql, null);

        // 删除完之后，应该查不到
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_2','vtab':'mysql_normaltbl_onegroup_mutilatom','realtabs':['mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += prefix + "_GROUP({groupIndex:0})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        List list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());
    }

    /**
     * OR条件
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_OR条件验证() throws Exception {
        // 使用or条件，会更新两个表
        String sql = prefix
                     + "({'type':'condition','vtab':'mysql_normaltbl_onegroup_mutilatom','params':[{'relation':'or','expr':['pk=0:int','pk=1:int']}]})*/";
        sql += "insert into mysql_normaltbl_onegroup_mutilatom values(?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        tddlUpdateData(sql, param);

        // 用直连库进行查询，查询两个表
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_2','vtab':'mysql_normaltbl_onegroup_mutilatom','realtabs':['mysql_normaltbl_onegroup_mutilatom_00']})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_2','vtab':'mysql_normaltbl_onegroup_mutilatom','realtabs':['mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 继续用规则hint查询
        sql = prefix
              + "({'type':'condition','vtab':'mysql_normaltbl_onegroup_mutilatom','params':[{'relation':'or','expr':['pk=0:int','pk=1:int']}]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        List list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(2, list.size());

        // 执行删除
        sql = prefix
              + "({'type':'condition','vtab':'mysql_normaltbl_onegroup_mutilatom','params':[{'relation':'or','expr':['pk=0:int','pk=1:int']}]})*/";
        sql += "delete from mysql_normaltbl_onegroup_mutilatom where pk = " + RANDOM_ID;
        tddlUpdateData(sql, null);

        // 删除完之后，应该查不到
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_2','vtab':'mysql_normaltbl_onegroup_mutilatom','realtabs':['mysql_normaltbl_onegroup_mutilatom_00']})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());

        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_2','vtab':'mysql_normaltbl_onegroup_mutilatom','realtabs':['mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());
    }

    /**
     * AND条件
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_and条件验证() throws Exception {
        // 使用or条件，会更新两个表
        String sql = prefix
                     + "({'type':'condition','vtab':'mysql_normaltbl_onegroup_mutilatom','params':[{'relation':'and','expr':['pk>=0:int','pk<2:int']}]})*/";
        sql += "insert into mysql_normaltbl_onegroup_mutilatom values(?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        tddlUpdateData(sql, param);

        // 用直连库进行查询，查询两个表
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_2','vtab':'mysql_normaltbl_onegroup_mutilatom','realtabs':['mysql_normaltbl_onegroup_mutilatom_00']})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_2','vtab':'mysql_normaltbl_onegroup_mutilatom','realtabs':['mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 继续用规则hint查询
        sql = prefix
              + "({'type':'condition','vtab':'mysql_normaltbl_onegroup_mutilatom','params':[{'relation':'and','expr':['pk>=0:int','pk<2:int']}]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        List list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(2, list.size());

        // 执行删除
        sql = prefix
              + "({'type':'condition','vtab':'mysql_normaltbl_onegroup_mutilatom','params':[{'relation':'and','expr':['pk>=0:int','pk<2:int']}]})*/";
        sql += "delete from mysql_normaltbl_onegroup_mutilatom where pk = " + RANDOM_ID;
        tddlUpdateData(sql, null);

        // 删除完之后，应该查不到
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_2','vtab':'mysql_normaltbl_onegroup_mutilatom','realtabs':['mysql_normaltbl_onegroup_mutilatom_00']})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());

        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_2','vtab':'mysql_normaltbl_onegroup_mutilatom','realtabs':['mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());
    }

    /**
     * batch测试
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_batch测试() throws Exception {
        String sql = prefix + "({'type':'condition','vtab':?,'params':[{'expr':[?]}]})*/";
        sql += "insert into mysql_normaltbl_onegroup_mutilatom values(?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 10; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add("mysql_normaltbl_onegroup_mutilatom");
            param.add("pk=" + (RANDOM_ID + i) + ":int");
            param.add(RANDOM_ID + i);
            param.add(RANDOM_INT);
            param.add(time);
            param.add(time);
            param.add(time);
            param.add(name);
            param.add(fl);
            params.add(param);
        }
        tddlUpdateDataBatch(sql, params);

        // 用hint查询
        for (int i = 0; i < 10; i++) {
            sql = prefix + "({'type':'condition','vtab':?,'params':[{'expr':[?]}]})*/";
            sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + (RANDOM_ID + i);
            List<Object> param = new ArrayList<Object>();
            param.add("mysql_normaltbl_onegroup_mutilatom");
            param.add("pk=" + (RANDOM_ID + i) + ":int");

            Map re = jdbcTemplate.queryForMap(sql, param.toArray(new Object[param.size()]));
            Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);
        }

        // 执行删除
        params = new ArrayList<List<Object>>();
        for (int i = 0; i < 10; i++) {
            sql = prefix + "({'type':'condition','vtab':?,'params':[{'expr':[?]}]})*/";
            sql += "delete from mysql_normaltbl_onegroup_mutilatom where pk = ?";
            List<Object> param = new ArrayList<Object>();
            param.add("mysql_normaltbl_onegroup_mutilatom");
            param.add("pk=" + (RANDOM_ID + i) + ":int");
            param.add(RANDOM_ID + i);
            params.add(param);
        }
        tddlUpdateDataBatch(sql, params);

        // 删除完之后，应该查不到
        for (int i = 0; i < 10; i++) {
            sql = prefix + "({'type':'condition','vtab':?,'params':[{'expr':[?]}]})*/";
            sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + (RANDOM_ID + i);
            List<Object> param = new ArrayList<Object>();
            param.add("mysql_normaltbl_onegroup_mutilatom");
            param.add("pk=" + (RANDOM_ID + i) + ":int");

            List list = jdbcTemplate.queryForList(sql, param.toArray(new Object[param.size()]));
            Assert.assertEquals(0, list.size());
        }
    }

    @Test
    public void testCorona() {
        String sql = String.format("/*!drds: $partitionOperand=('pk'=%d), $table='%s' */",
            RANDOM_ID,
            normaltblTableName);
        sql += String.format("insert into %s(pk,id,gmt_create,name,floatCol) values(?,?,?,?,?)", normaltblTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(name);
        param.add(fl);
        try {
            tddlUpdateData(sql, param);
        } catch (Exception e) {
            e.printStackTrace();

        }
    }
}
