package com.taobao.tddl.qatest.matrix.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * LeftJoin测试，bdb不支持LeftJoin，只有当dbType="mysql"时测试用例才会运行
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class LeftJoinTest extends BaseMatrixTestCase {

    String[] columnParam = { "host_id", "host_name", "hostgroup_id", "hostgroup_name" };

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableName.mysqlHostinfoHostgroupTable(dbType));
    }

    public LeftJoinTest(String monitor_host_infoTableName, String monitor_hostgroup_infoTableName) throws Exception{
        BaseTestCase.host_info = monitor_host_infoTableName;
        BaseTestCase.hostgroup = monitor_hostgroup_infoTableName;
        initData();
    }

    public void initData() throws Exception {

        hostgroupPrepare(0, 10);
        hostinfoPrepare(5, 20);
        hostinfoDataAdd(20, 30, 8l);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    /**
     * leftJoin测试
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup + ".hostgroup_name from " + hostgroup + " left join " + host_info + "  " + "on "
                     + hostgroup + ".hostgroup_id=" + host_info + ".hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * leftJoin测试 加where条件
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithWhereTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup + ".hostgroup_name from " + hostgroup + " left join " + host_info + "  " + "on "
                     + hostgroup + ".hostgroup_id=" + host_info + ".hostgroup_id where " + hostgroup
                     + ".hostgroup_name='hostgroupname0'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 带count的leftJoin测试 where条件为like
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithCountLikeTest() throws Exception {
        String sql = "select count(*) from " + hostgroup + " left join " + host_info + "  " + "on " + hostgroup
                     + ".hostgroup_id=" + host_info + ".hostgroup_id where " + hostgroup + ".hostgroup_name like ?";
        List<Object> param = new ArrayList<Object>();
        param.add("hostgroupname%");
        String[] columnParam = { "count(*)" };
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * leftJoin测试 where条件中为and
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithAndTest() throws Exception {

        if (hostgroup.startsWith("ob") && host_info.startsWith("ob")) {
            // TODO:ob outter join 有bug
            // where中的条件会在join之前就进行过滤
            return;
        }
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup + ".hostgroup_name from " + hostgroup + " left join " + host_info + "  " + "on "
                     + hostgroup + ".hostgroup_id=" + host_info + ".hostgroup_id where " + hostgroup
                     + ".hostgroup_name='hostgroupname0'" + " and " + host_info + ".host_name='hostname0'";

        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * leftJoin测试 where条件中为orS
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithOrTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup + ".hostgroup_name from " + hostgroup + " left join " + host_info + "  " + "on "
                     + hostgroup + ".hostgroup_id=" + host_info + ".hostgroup_id where " + hostgroup
                     + ".hostgroup_name='hostgroupname0'" + " OR " + hostgroup + ".hostgroup_name='hostgroupname1'";

        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * leftJoin测试 where条件中为and(* or host_name is null)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithAndOrTest() throws Exception {
        String sql = "select * from " + hostgroup + " left join " + host_info + " on " + hostgroup + ".hostgroup_id="
                     + host_info + ".hostgroup_id where " + hostgroup + ".hostgroup_name like 'hostgroupname%'"
                     + " and  (" + hostgroup + ".hostgroup_name='hostgroupname1' or " + host_info
                     + ".host_name is null)";

        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * leftJoin测试 where条件中为and(* or host_name is true)
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithAndOrTrueTest() throws Exception {
        if (hostgroup.startsWith("ob") && host_info.startsWith("ob")) {
            // ob is true和mysql不太一致
            // where中的条件会在join之前就进行过滤
            return;
        }

        String sql = "select * from " + hostgroup + " left join " + host_info + " on " + hostgroup + ".hostgroup_id="
                     + host_info + ".hostgroup_id where " + hostgroup + ".hostgroup_name like 'hostgroupname%'"
                     + " and  (" + hostgroup + ".hostgroup_name='hostgroupname1' or " + host_info
                     + ".host_name is true)";

        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * leftJoin测试 where条件后带limit
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithLimitTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup + ".hostgroup_name from " + hostgroup + " left join " + host_info + "  " + "on "
                     + hostgroup + ".hostgroup_id=" + host_info + ".hostgroup_id where " + hostgroup
                     + ".hostgroup_name='hostgroupname0'" + " limit 1";

        selectConutAssert(sql, Collections.EMPTY_LIST);
    }

    /**
     * leftJoin测试 表使用不带as的别名
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithAliasTest() throws Exception {
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + hostgroup + " b left join "
                     + host_info + " a " + "on b.hostgroup_id=a.hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * leftJoin测试 表使用带as的别名
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithAliasAsTest() throws Exception {
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + hostgroup
                     + " as b left join " + host_info + " as a " + "on b.hostgroup_id=a.hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * leftJoin测试 带 orderby 左键
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithOrderByTest() throws Exception {
        // 左表有序，可下推
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from "
                     + hostgroup
                     + " as b left join "
                     + host_info
                     + " as a "
                     + "on b.hostgroup_id=a.hostgroup_id where b.hostgroup_name='hostgroupname0' order by b.hostgroup_id";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + hostgroup + " as b left join "
              + host_info + " as a "
              + "on b.hostgroup_id=a.hostgroup_id where b.hostgroup_name='hostgroupname0' order by b.hostgroup_id asc";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + hostgroup + " as b left join "
              + host_info + " as a "
              + "on b.hostgroup_id=a.hostgroup_id where b.hostgroup_name='hostgroupname0' order by b.hostgroup_id desc";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * leftJoin测试 orderby右外键
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithOrderByRightOutterTest() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from "
                     + hostgroup
                     + " as b left join "
                     + host_info
                     + " as a "
                     + "on b.hostgroup_id=a.hostgroup_id where b.hostgroup_name='hostgroupname0' order by a.host_id";
        // selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        // 使用sort merge join，不需要临时表排序
        sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + hostgroup + " as b left join "
              + host_info + " as a " + "on b.hostgroup_id=a.hostgroup_id order by a.hostgroup_id asc , a.host_id asc";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        // 使用sort merge join，不需要临时表排序
        sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + hostgroup + " as b left join "
              + host_info + " as a "
              + "on b.hostgroup_id=a.hostgroup_id where b.hostgroup_name='hostgroupname0' order by b.hostgroup_id desc";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * null的排序bdb和mysql理解不一样 bdb 理解的是最大的， mysql理解的是最小的
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithOrderLimitTest() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from "
                     + hostgroup
                     + " as b left join "
                     + host_info
                     + " as a "
                     + "on b.hostgroup_id=a.hostgroup_id where  b.hostgroup_name like 'hostgroupname%' order by a.host_id,b.hostgroup_name";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from "
              + hostgroup
              + " as b left join "
              + host_info
              + " as a "
              + "on b.hostgroup_id=a.hostgroup_id where  b.hostgroup_name like 'hostgroupname%' order by a.host_id,b.hostgroup_name desc";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * left join 查询的时候 使用getObject()
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithGetByIndexTest() throws Exception {
        int columnParam = 5;
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from "
                     + hostgroup
                     + " as b left join "
                     + host_info
                     + " as a "
                     + "on b.hostgroup_id=a.hostgroup_id where  b.hostgroup_name like 'hostgroupname%' limit 1,10 ";
        selectContentSameAssertByIndex(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * left join 带子查询
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void leftJoinWithSubQueryTest() throws Exception {
        if (hostgroup.startsWith("ob") && host_info.startsWith("ob")) {
            // TODO:ob join bug，对别名支持不好
            return;
        }

        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ SELECT sumId ,host_name , a.hostgroup_id as aid ,b.hostgroup_id  as bid from "
                     + "( select  sum(host_id) as sumId,host_name,hostgroup_id from "
                     + host_info
                     + " where host_id BETWEEN ? and ? GROUP BY host_name ORDER BY hostgroup_id LIMIT ?) as a"
                     + " LEFT JOIN (SELECT SUM(hostgroup_id) , hostgroup_name,hostgroup_id  from "
                     + hostgroup
                     + " where hostgroup_id "
                     + "BETWEEN ? and ? GROUP BY hostgroup_name) as b ON a.hostgroup_id=b.hostgroup_id ORDER BY sumId DESC";
        List<Object> param = new ArrayList<Object>();
        param.add(0);
        param.add(100);
        param.add(20);
        param.add(1);
        param.add(20);
        String[] columnParam = { "sumId", "host_name", "aid", "bid" };
        selectContentSameAssert(sql, columnParam, param);
    }
}
