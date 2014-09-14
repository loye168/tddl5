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
 * 直接hint
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
public class DirectHintTest extends BaseMatrixTestCase {

    private JdbcTemplate jdbcTemplate;
    private Date         time   = new Date();
    private String       prefix = "/*+TDDL";

    public DirectHintTest(){
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
     * 指定库 不指定表
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_指定库_不指定表() throws Exception {
        String sql = prefix + "({'type':'direct','dbid':'andor_mysql_group_oneAtom'})*/";
        sql += "insert into mysql_normaltbl_oneGroup_oneAtom values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        tddlUpdateData(sql, param);

        sql = "select gmt_timestamp from " + normaltblTableName + " where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("gmt_timestamp")).getTime() / 1000);

        sql = prefix + "({'type':'direct','dbid':'andor_mysql_group_oneAtom'})*/";
        sql += "delete from mysql_normaltbl_oneGroup_oneAtom where pk = " + RANDOM_ID;
        tddlUpdateData(sql, null);
    }

    /**
     * 指定库 指定表
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_指定库_指定表() throws Exception {
        // 源表为mysql_normaltbl_oneGroup_oneAtom, 指定两个表
        // mysql_normaltbl_onegroup_mutilatom_00 ，
        // mysql_normaltbl_onegroup_mutilatom_01
        String sql = prefix
                     + "({'type':'direct','dbid':'andor_mysql_group_oneAtom','vtab':'mysql_normaltbl_oneGroup_oneAtom','realtabs':['mysql_normaltbl_onegroup_mutilatom_00','mysql_normaltbl_onegroup_mutilatom_01']})*/ ";
        sql += "insert into mysql_normaltbl_oneGroup_oneAtom values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        tddlUpdateData(sql, param);

        // 分别查询两个库
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_oneAtom','vtab':'mysql_normaltbl_oneGroup_oneAtom','realtabs':['mysql_normaltbl_onegroup_mutilatom_00']})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_oneAtom','vtab':'mysql_normaltbl_oneGroup_oneAtom','realtabs':['mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 删除
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_oneAtom','vtab':'mysql_normaltbl_oneGroup_oneAtom','realtabs':['mysql_normaltbl_onegroup_mutilatom_00','mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += "delete from mysql_normaltbl_oneGroup_oneAtom where pk = " + RANDOM_ID;
        tddlUpdateData(sql, null);
    }

    /**
     * 指定库 指定表 多表名使用别名替换
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_指定库_指定表_多表名替换() throws Exception {
        String sql = prefix
                     + "({'type':'direct','dbid':'andor_mysql_group_oneAtom','vtab':'mysql_normaltbl_oneGroup_oneAtom','realtabs':['mysql_normaltbl_onegroup_mutilatom_00','mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += "insert into mysql_normaltbl_oneGroup_oneAtom values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        tddlUpdateData(sql, param);

        // 多表名替换时，用逗号分隔
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_oneAtom','vtab':'tablea,tableb','realtabs':['mysql_normaltbl_onegroup_mutilatom_00,mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += "select a.gmt_timestamp as atime,b.gmt_timestamp as btime from tablea a inner join tableb b on a.pk = b.pk where a.pk="
               + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("ATIME")).getTime() / 1000);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("BTIME")).getTime() / 1000);

        // 删除
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_oneAtom','vtab':'mysql_normaltbl_oneGroup_oneAtom','realtabs':['mysql_normaltbl_onegroup_mutilatom_00','mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += "delete from mysql_normaltbl_oneGroup_oneAtom where pk = " + RANDOM_ID;
        tddlUpdateData(sql, null);
    }

    /**
     * 指定库 指定表 绑定变量
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_指定库_指定表_绑定变量() throws Exception {
        // 源表为mysql_normaltbl_oneGroup_oneAtom, 指定两个表
        // mysql_normaltbl_onegroup_mutilatom_00 ，
        // mysql_normaltbl_onegroup_mutilatom_01
        String sql = prefix + "({'type':'direct','dbid': ?  ,'vtab':?,'realtabs':[?,?]})*/ ";
        sql += "insert into mysql_normaltbl_oneGroup_oneAtom values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("andor_mysql_group_oneAtom");
        param.add("mysql_normaltbl_oneGroup_oneAtom");
        param.add("mysql_normaltbl_onegroup_mutilatom_00");
        param.add("mysql_normaltbl_onegroup_mutilatom_01");
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        tddlUpdateData(sql, param);

        // 分别查询两个库
        sql = prefix + "({'type':'direct','dbid':?,'vtab':?,'realtabs':[?]})*/ ";
        sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        Object args0[] = { "andor_mysql_group_oneAtom", "mysql_normaltbl_oneGroup_oneAtom",
                "mysql_normaltbl_onegroup_mutilatom_00" };
        Map re = jdbcTemplate.queryForMap(sql, args0);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        sql = prefix + "({'type':'direct','dbid':?,'vtab':?,'realtabs':[?]})*/ ";
        sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        Object args1[] = { "andor_mysql_group_oneAtom", "mysql_normaltbl_oneGroup_oneAtom",
                "mysql_normaltbl_onegroup_mutilatom_01" };
        re = jdbcTemplate.queryForMap(sql, args1);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 删除
        sql = prefix + "({'type':'direct','dbid':?,'vtab':?,'realtabs':[?,?]})*/ ";
        sql += "delete from mysql_normaltbl_oneGroup_oneAtom where pk = " + RANDOM_ID;
        param = new ArrayList<Object>();
        param.add("andor_mysql_group_oneAtom");
        param.add("mysql_normaltbl_oneGroup_oneAtom");
        param.add("mysql_normaltbl_onegroup_mutilatom_00");
        param.add("mysql_normaltbl_onegroup_mutilatom_01");
        tddlUpdateData(sql, param);
    }

    /**
     * 指定库 指定表 选择groupindex
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_指定库_指定表_选择groupindex() throws Exception {
        // 源表为mysql_normaltbl_oneGroup_oneAtom, 指定两个表
        // mysql_normaltbl_onegroup_mutilatom_00 ，
        // mysql_normaltbl_onegroup_mutilatom_01
        String sql = prefix
                     + "({'type':'direct','dbid':'andor_mysql_group_oneAtom','vtab':'mysql_normaltbl_oneGroup_oneAtom','realtabs':['mysql_normaltbl_onegroup_mutilatom_00','mysql_normaltbl_onegroup_mutilatom_01']})*/ ";
        sql += prefix + "_GROUP({groupIndex:0})*/";
        sql += "insert into mysql_normaltbl_oneGroup_oneAtom values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        tddlUpdateData(sql, param);

        // 分别查询两个库
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_oneAtom','vtab':'mysql_normaltbl_oneGroup_oneAtom','realtabs':['mysql_normaltbl_onegroup_mutilatom_00']})*/";
        sql += prefix + "_GROUP({groupIndex:0})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_oneAtom','vtab':'mysql_normaltbl_oneGroup_oneAtom','realtabs':['mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += prefix + "_GROUP({groupIndex:0})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 删除
        sql = prefix
              + "({'type':'direct','dbid':'andor_mysql_group_oneAtom','vtab':'mysql_normaltbl_oneGroup_oneAtom','realtabs':['mysql_normaltbl_onegroup_mutilatom_00','mysql_normaltbl_onegroup_mutilatom_01']})*/";
        sql += prefix + "_GROUP({groupIndex:0})*/";
        sql += "delete from mysql_normaltbl_oneGroup_oneAtom where pk = " + RANDOM_ID;
        tddlUpdateData(sql, null);
    }

    /**
     * batch测试
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_batch测试() throws Exception {
        // 源表为mysql_normaltbl_oneGroup_oneAtom, 指定两个表
        // mysql_normaltbl_onegroup_mutilatom_00 ，
        // mysql_normaltbl_onegroup_mutilatom_01
        String sql = prefix + "({'type':'direct','dbid':?,'vtab':?,'realtabs':[?]})*/ ";
        sql += "insert into mysql_normaltbl_oneGroup_oneAtom values(?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 10; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add("andor_mysql_group_oneAtom");
            param.add("mysql_normaltbl_oneGroup_oneAtom");
            param.add("mysql_normaltbl_onegroup_mutilatom_0" + i % 2);
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

        // 分别查询两个库
        for (int i = 0; i < 10; i += 2) {
            sql = prefix + "({'type':'direct','dbid':?,'vtab':?,'realtabs':[?]})*/ ";
            sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + (RANDOM_ID + i);
            Object args0[] = { "andor_mysql_group_oneAtom", "mysql_normaltbl_oneGroup_oneAtom",
                    "mysql_normaltbl_onegroup_mutilatom_00" };
            Map re = jdbcTemplate.queryForMap(sql, args0);
            Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);
        }

        for (int i = 1; i < 10; i += 2) {
            sql = prefix + "({'type':'direct','dbid':?,'vtab':?,'realtabs':[?]})*/ ";
            sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + (RANDOM_ID + i);
            Object args1[] = { "andor_mysql_group_oneAtom", "mysql_normaltbl_oneGroup_oneAtom",
                    "mysql_normaltbl_onegroup_mutilatom_01" };
            Map re = jdbcTemplate.queryForMap(sql, args1);
            Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);
        }

        // 删除
        params = new ArrayList<List<Object>>();
        for (int i = 0; i < 10; i++) {
            sql = prefix + "({'type':'direct','dbid':?,'vtab':?,'realtabs':[?]})*/ ";
            sql += "delete from mysql_normaltbl_oneGroup_oneAtom where pk = ?";
            List<Object> param = new ArrayList<Object>();
            param.add("andor_mysql_group_oneAtom");
            param.add("mysql_normaltbl_oneGroup_oneAtom");
            param.add("mysql_normaltbl_onegroup_mutilatom_0" + i % 2);
            param.add(RANDOM_ID + i);

            params.add(param);
        }
        tddlUpdateDataBatch(sql, params);

        // 应该找不到了
        for (int i = 0; i < 10; i += 2) {
            sql = prefix + "({'type':'direct','dbid':?,'vtab':?,'realtabs':[?]})*/ ";
            sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + (RANDOM_ID + i);
            Object args0[] = { "andor_mysql_group_oneAtom", "mysql_normaltbl_oneGroup_oneAtom",
                    "mysql_normaltbl_onegroup_mutilatom_00" };
            List list = jdbcTemplate.queryForList(sql, args0);
            Assert.assertEquals(0, list.size());
        }

        for (int i = 1; i < 10; i += 2) {
            sql = prefix + "({'type':'direct','dbid':?,'vtab':?,'realtabs':[?]})*/ ";
            sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + (RANDOM_ID + i);
            Object args1[] = { "andor_mysql_group_oneAtom", "mysql_normaltbl_oneGroup_oneAtom",
                    "mysql_normaltbl_onegroup_mutilatom_01" };
            List list = jdbcTemplate.queryForList(sql, args1);
            Assert.assertEquals(0, list.size());
        }

    }
}
