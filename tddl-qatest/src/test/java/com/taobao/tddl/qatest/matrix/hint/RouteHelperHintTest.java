package com.taobao.tddl.qatest.matrix.hint;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.tddl.client.RouteCondition.ROUTE_TYPE;
import com.taobao.tddl.common.GroupDataSourceRouteHelper;
import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.util.RouteHelper;

/**
 * @author jianghang 2014-6-11 下午10:10:11
 * @since 5.1.0
 */
public class RouteHelperHintTest extends BaseMatrixTestCase {

    private JdbcTemplate jdbcTemplate;
    private Date         time = new Date();

    public RouteHelperHintTest(){
        BaseTestCase.normaltblTableName = "mysql_normaltbl_oneGroup_oneAtom";
        jdbcTemplate = new JdbcTemplate(tddlDatasource);
    }

    @Before
    public void initData() throws Exception {
        tddlUpdateData("delete from mysql_normaltbl_oneGroup_oneAtom", null);
        tddlUpdateData("delete from mysql_normaltbl_onegroup_mutilatom", null);
    }

    @Test
    public void test_指定db() throws Exception {
        if (isTddlServer()) {
            // 不测是server
            return;
        }

        RouteHelper.executeByDBAndTab("andor_mysql_group_oneAtom",
            normaltblTableName,
            "mysql_normaltbl_onegroup_mutilatom_00",
            "mysql_normaltbl_onegroup_mutilatom_01");
        String sql = "insert into mysql_normaltbl_oneGroup_oneAtom values(?,?,?,?,?,?,?)";
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
        RouteHelper.executeByDBAndTab("andor_mysql_group_oneAtom",
            normaltblTableName,
            "mysql_normaltbl_onegroup_mutilatom_00");
        sql = "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        RouteHelper.executeByDBAndTab("andor_mysql_group_oneAtom",
            normaltblTableName,
            "mysql_normaltbl_onegroup_mutilatom_01");
        sql = "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 删除
        RouteHelper.executeByDBAndTab("andor_mysql_group_oneAtom",
            normaltblTableName,
            "mysql_normaltbl_onegroup_mutilatom_00",
            "mysql_normaltbl_onegroup_mutilatom_01");
        sql = "delete from mysql_normaltbl_oneGroup_oneAtom where pk = " + RANDOM_ID;
        tddlUpdateData(sql, null);

        // 应该查不到了
        RouteHelper.executeByDBAndTab("andor_mysql_group_oneAtom",
            normaltblTableName,
            "mysql_normaltbl_onegroup_mutilatom_00",
            "mysql_normaltbl_onegroup_mutilatom_01");
        sql = "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        List list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());
    }

    /**
     * 简单等值条件
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_简单等值条件() throws Exception {
        RouteHelper.executeByCondition("mysql_normaltbl_onegroup_mutilatom", "pk", 1);
        String sql = "insert into mysql_normaltbl_onegroup_mutilatom values(?,?,?,?,?,?,?)";

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
        RouteHelper.executeByDBAndTab("andor_mysql_group_2",
            "mysql_normaltbl_onegroup_mutilatom",
            "mysql_normaltbl_onegroup_mutilatom_01");
        sql = "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 继续用规则hint查询
        RouteHelper.executeByCondition("mysql_normaltbl_onegroup_mutilatom",
            "pk",
            1,
            ROUTE_TYPE.FLUSH_ON_CLOSECONNECTION);
        sql = "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        ResultSet rs = tddlQueryData(sql, null);
        rs.next();
        Assert.assertEquals(time.getTime() / 1000, ((Date) rs.getTimestamp("GMT_TIMESTAMP")).getTime() / 1000);

        // 执行删除
        sql = "delete from mysql_normaltbl_onegroup_mutilatom where pk = " + RANDOM_ID;
        tddlUpdateData(sql, null);

        // 删除完之后，应该查不到
        RouteHelper.executeByCondition("mysql_normaltbl_onegroup_mutilatom", "pk", 1);
        sql = "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        List list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());
    }

    @Test
    public void test_group测试() throws Exception {
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        String sql = "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom order by pk";
        List list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());
    }

}
