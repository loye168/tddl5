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
 * group hint
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
public class GroupHintTest extends BaseMatrixTestCase {

    private JdbcTemplate jdbcTemplate;
    private Date         time   = new Date();
    private String       prefix = "/*+TDDL";

    public GroupHintTest(){
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
     * 指定groupIndex
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void test_指定groupIndex() throws Exception {
        String sql = prefix + "_GROUP({groupIndex:0})*/";
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

        // 查询一次
        sql = prefix + "_GROUP({groupIndex:0})*/";
        sql += "select gmt_timestamp from " + normaltblTableName + " where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("gmt_timestamp")).getTime() / 1000);

        // 删除
        sql = prefix + "_GROUP({groupIndex:0})*/";
        sql += "delete from mysql_normaltbl_oneGroup_oneAtom where pk = " + RANDOM_ID;
        tddlUpdateData(sql, null);

        sql = prefix + "_GROUP({groupIndex:0})*/";
        sql += "select gmt_timestamp from " + normaltblTableName + " where pk=" + RANDOM_ID;
        List list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());
    }
}
