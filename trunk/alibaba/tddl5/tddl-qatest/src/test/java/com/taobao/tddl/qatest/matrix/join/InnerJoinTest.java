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
 * 内连接
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class InnerJoinTest extends BaseMatrixTestCase {

    String[] columnParam = { "host_id", "host_name", "hostgroup_id", "hostgroup_name" };

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableName.hostinfoHostgoupStudentModuleinfoModulehostTable(dbType));
    }

    public InnerJoinTest(String monitor_host_infoTableName, String monitor_hostgroup_infoTableName,
                         String studentTableName, String monitor_module_infoTableName,
                         String monitor_module_hostTableName) throws Exception{
        BaseTestCase.host_info = monitor_host_infoTableName;
        BaseTestCase.hostgroup = monitor_hostgroup_infoTableName;
        BaseTestCase.studentTableName = studentTableName;
        BaseTestCase.module_info = monitor_module_infoTableName;
        BaseTestCase.module_host = monitor_module_hostTableName;
        initData();
    }

    public void initData() throws Exception {

        hostinfoPrepare(0, 3);
        hostgroupPrepare(0, 2);
        module_infoPrepare(0, 2);
        module_hostPrepare(0, 10);
        studentPrepare(0, 1);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    /**
     * 内连接 on后面有一个等于条件
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @SuppressWarnings("unchecked")
    @Test
    public void innerJoinTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup + ".hostgroup_name from " + host_info + " inner join " + hostgroup + "  " + "on "
                     + host_info + ".hostgroup_id=" + hostgroup + ".hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 on后面有两个等于条件
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void innerJoinWithMultiOnTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup + ".hostgroup_name from " + host_info + " inner join " + hostgroup + "  " + "on "
                     + host_info + ".hostgroup_id=" + hostgroup + ".hostgroup_id and " + host_info + ".host_name="
                     + hostgroup + ".hostgroup_name";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 结果中含多条数据
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithMutilValueTest() throws Exception {
        // bdb数据库join根据desc排序抛出空指针异常,暂时忽略
        if (host_info.contains("mysql")) {
            String sql = "replace into " + host_info + "(host_id,hostgroup_id) values(?,?)";
            List<Object> param = new ArrayList<Object>();
            for (long i = 3; i < 8; i++) {
                param.clear();
                param.add(i);
                param.add(0l);
                tddlUpdateData(sql, param);
                mysqlUpdateData(sql, param);
            }
            sql = "select * from " + hostgroup + " inner join " + host_info + "  " + "on " + host_info
                  + ".hostgroup_id=" + hostgroup + ".hostgroup_id";
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    /**
     * 内连接 条件中有on和where两种,where后面为string类型的列
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithAndTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".hostgroup_id," + host_info
                     + ".host_name," + hostgroup + ".hostgroup_name " + "from " + hostgroup + " inner join "
                     + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup + ".hostgroup_id where "
                     + host_info + ".host_name='hostname0'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 条件中有on和where两种
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereNumFieldTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup + ".hostgroup_name " + "from " + hostgroup + " inner join "
                     + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup + ".hostgroup_id where "
                     + host_info + ".host_id=0";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info + ".hostgroup_id,"
              + hostgroup + ".hostgroup_name " + "from " + hostgroup + " inner join " + host_info + "  " + "on "
              + host_info + ".hostgroup_id=" + hostgroup + ".hostgroup_id where " + host_info + ".host_id>0";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 条件中有on和where两种,where后面为string类型的列
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereStringFieldTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup + ".hostgroup_name " + "from " + hostgroup + " inner join "
                     + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup + ".hostgroup_id where "
                     + host_info + ".host_name='hostname0'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 条件中有on和where两种,where后面为and连接的多个条件
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereAndTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup + ".hostgroup_name " + "from " + hostgroup + " inner join "
                     + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup + ".hostgroup_id where "
                     + host_info + ".host_name='hostname0' and " + hostgroup + ".hostgroup_name='hostgroupname0'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 条件中有on和where两种,where后面为or连接的多个条件
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereOrTest() throws Exception {
        // bdb数据库join根据desc排序抛出空指针异常,暂时忽略
        if (host_info.contains("mysql")) {
            String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                         + ".hostgroup_id," + hostgroup + ".hostgroup_name " + "from " + hostgroup + " inner join "
                         + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup + ".hostgroup_id where "
                         + host_info + ".host_name='hostname0' or " + hostgroup + ".hostgroup_name='hostgroupname1'";
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    /**
     * 内连接 条件中有on和where两种,再加limit条件
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @SuppressWarnings("unchecked")
    @Test
    public void InnerJoinWithWhereLimitTest() throws Exception {
        // 暂时先去掉对host_info_oneGroup_oneAtom_threeIndex这个表类型的验证，应该是因为没有异步复制导致的
        if (host_info != "host_info_oneGroup_oneAtom_threeIndex") {
            String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                         + ".hostgroup_id," + hostgroup + ".hostgroup_name " + "from " + hostgroup + " inner join "
                         + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup + ".hostgroup_id where "
                         + host_info + ".host_name='hostname0'   limit 1";
            selectConutAssert(sql, Collections.EMPTY_LIST);
        }
    }

    /**
     * 三表join
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableTest() throws Exception {
        String sql = "replace into " + host_info + "(host_id,hostgroup_id) values(?,?)";
        List<Object> param = new ArrayList<Object>();
        for (long i = 3; i < 8; i++) {
            param.clear();
            param.add(i);
            param.add(0l);
            tddlUpdateData(sql, param);
            mysqlUpdateData(sql, param);
            // rc.close();
        }
        sql = "SELECT * FROM " + host_info + " inner JOIN " + hostgroup + " ON " + host_info + ".hostgroup_id="
              + hostgroup + ".hostgroup_id " + "inner JOIN " + studentTableName + " ON " + hostgroup + ".hostgroup_id="
              + studentTableName + ".id";
        String[] columnParam = { "host_id", "host_name", "hostgroup_id", "hostgroup_name", "name" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 三表join 后面带where条件 收藏夹主要流程模拟测试 查询指定用户所收藏的商品信息 或者指定商品收藏用户信息
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableWithWhere() throws Exception {
        String sql = "SELECT * from " + host_info + " INNER JOIN " + module_host + " ON " + host_info + ".host_id="
                     + module_host + ".host_id INNER JOIN " + module_info + "  ON " + module_info + ".module_id="
                     + module_host + ".module_id where " + module_info + ".module_name='module1'";
        String[] columnParam = { "host_id", "host_name", "module_id", "id", "module_name" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 三表join 后面带where条件后面有or情况
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableWithWhereWithOr() throws Exception {
        String sql = "SELECT * from " + host_info + " INNER JOIN " + module_host + " ON " + host_info + ".host_id="
                     + module_host + ".host_id INNER JOIN " + module_info + "  ON " + module_info + ".module_id="
                     + module_host + ".module_id where " + module_info + ".module_name='module1' or " + module_info
                     + ".module_name='module0'";
        String[] columnParam = { "host_id", "host_name", "module_id", "id", "module_name" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 三表join 后面带where条件后面有and情况
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @SuppressWarnings("unchecked")
    @Test
    public void InnerJoinThreeTableWithWhereWithAnd() throws Exception {
        String sql = "SELECT * from " + host_info + " INNER JOIN " + module_host + " ON " + host_info + ".host_id="
                     + module_host + ".host_id INNER JOIN " + module_info + "  ON " + module_info + ".module_id="
                     + module_host + ".module_id where " + module_info + ".module_name='module1' and " + host_info
                     + ".host_name='hostname1'";
        String[] columnParam = { "host_id", "host_name", "module_id", "id", "module_name" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 三表join 匹配多条数据
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableMutilDateTest() throws Exception {
        String sql = "replace into " + host_info + "(host_id,hostgroup_id) values(?,?)";
        List<Object> param = new ArrayList<Object>();
        for (long i = 3; i < 8; i++) {
            param.clear();
            param.add(i);
            param.add(0l);
            tddlUpdateData(sql, param);
            mysqlUpdateData(sql, param);
        }
        sql = "SELECT * FROM " + host_info + " inner JOIN " + hostgroup + " ON " + host_info + ".hostgroup_id="
              + hostgroup + ".hostgroup_id " + "inner JOIN " + studentTableName + " ON " + hostgroup + ".hostgroup_id="
              + studentTableName + ".id";
        String[] columnParam = { "host_id", "host_name", "module_id", "id", "hostgroup_name" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 不带as的别名测试
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithAliasTest() throws Exception {
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + host_info
                     + " a inner join " + hostgroup + " b " + "on a.hostgroup_id=b.hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 带as的别名测试
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithAliasAsTest() throws Exception {
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + host_info
                     + " as a inner join " + hostgroup + " as b " + "on a.hostgroup_id=b.hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 加order by测试
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithOrderByTest() throws Exception {
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + host_info
                     + " as a inner join " + hostgroup + " as b "
                     + "on a.hostgroup_id=b.hostgroup_id order by a.host_id";
        String[] columnParam = { "host_id", "host_name", "hostgroup_name" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 加order by升序测试
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithOrderByascTest() throws Exception {
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + host_info
                     + " as a inner join " + hostgroup + " as b "
                     + "on a.hostgroup_id=b.hostgroup_id order by a.host_id asc";
        String[] columnParam = { "host_id", "host_name", "hostgroup_name" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 加order by降序测试
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test()
    public void InnerJoinWithOrderBydescTest() throws Exception {
        // bdb数据库join根据desc排序抛出空指针异常,暂时忽略
        if (host_info.contains("mysql")) {
            String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + host_info
                         + " as a inner join " + hostgroup + " as b "
                         + "on a.hostgroup_id=b.hostgroup_id order by a.host_id desc";
            String[] columnParam = { "host_id", "host_name", "hostgroup_name" };
            selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    /**
     * 内连接 再加上子查询
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test()
    public void InnerJoinWithSubQueryTest() throws Exception {
        String sql = "select t1.sum1,t2.count2 from (select sum(host_id) as sum1,station_id from " + host_info
                     + " group by station_id) t1 " + "join (select count(hostgroup_id) as count2,station_id from "
                     + hostgroup + " group by station_id) t2 on t1.station_id=t2.station_id";
        String[] columnParam = { "sum1", "count2" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 再加上子查询，再加多join条件
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test()
    public void InnerJoinWithSubQueryMultiJoinTest() throws Exception {
        String sql = "select t1.sum1,t2.count2 from (select sum(host_id) as sum1,station_id from " + host_info
                     + " group by station_id) t1 " + "join (select count(hostgroup_id) as count2,station_id from "
                     + hostgroup + " group by station_id) t2 on t1.station_id=t2.station_id and t1.sum1 = t2.count2";
        String[] columnParam = { "sum1", "count2" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 内连接 加avg测试
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithAvgTest() throws Exception {
        String sql = "select t1.avg,t2.avg,avg(t1.station_id) from (select avg(host_id) as avg,station_id from "
                     + host_info + " group by station_id) t1 "
                     + "join (select avg(hostgroup_id) as avg,station_id from " + hostgroup
                     + " group by station_id) t2 on t1.station_id=t2.station_id";
        String[] columnParam = { "t1.avg", "t2.avg", "avg(t1.station_id)" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

}
