package com.taobao.tddl.optimizer.costbased;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.IQueryTree.LOCK_MODE;
import com.taobao.tddl.optimizer.core.plan.IQueryTree.QUERY_CONCURRENCY;
import com.taobao.tddl.optimizer.core.plan.bean.QueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.exception.SqlParserException;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 整个优化器的集成测试，主要是一些query
 */
public class OptimizerTest extends BaseOptimizerTest {

    private static Map<String, Object> extraCmd = new HashMap<String, Object>();

    @BeforeClass
    public static void setUp() {
        extraCmd.put(ConnectionProperties.CHOOSE_INDEX, true);
        extraCmd.put(ConnectionProperties.CHOOSE_JOIN, false);
        extraCmd.put(ConnectionProperties.CHOOSE_INDEX_MERGE, false);
        // extraCmd.put(ConnectionProperties.MERGE_EXPAND, false);
        extraCmd.put(ConnectionProperties.JOIN_MERGE_JOIN_JUDGE_BY_RULE, true);
    }

    @Test
    public void test_单表查询_无条件() {
        TableNode table = new TableNode("TABLE1");
        QueryTreeNode qn = table;
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(qn, null, extraCmd);

        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.SEQUENTIAL, ((IMerge) qc).getQueryConcurrency());// 串行
        IDataNodeExecutor dne = ((IMerge) qc).getSubNodes().get(0);
        Assert.assertTrue(dne instanceof IQuery);
        IQuery query = (IQuery) dne;
        Assert.assertEquals(null, query.getKeyFilter());
        Assert.assertEquals(null, query.getValueFilter());
    }

    // 单表主键查询
    // ID为主键，同时在ID上存在索引
    // 直接查询KV ID->data
    // keyFilter为ID=1
    @Test
    public void test_单表查询_主键条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID=1 AND ID<40");

        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals("TABLE1.ID = 1", ((IQuery) qc).getKeyFilter().toString());
        Assert.assertEquals(null, ((IQuery) qc).getValueFilter());
    }

    // 单表主键查询
    // ID为主键，同时在ID上存在索引
    // 因为!=不能使用主键索引
    // valueFilter为ID!=1
    @Test
    public void test_单表查询_主键条件_不等于只能是valueFilter() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID != 1");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);

        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.GROUP_CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        IDataNodeExecutor dne = ((IMerge) qc).getSubNodes().get(0);
        Assert.assertTrue(dne instanceof IQuery);
        IQuery query = (IQuery) dne;
        Assert.assertEquals(null, query.getKeyFilter());
        Assert.assertEquals("TABLE1.ID != 1", query.getValueFilter().toString());
    }

    // 单表非主键索引查询
    // NAME上存在索引
    // 会生成一个Join节点
    // 左边通过NAME索引找到满足条件的PK，keyFilter应该为NAME=1
    // 与pk->data Join
    // Join类型为IndexNestLoop
    @Test
    public void test_单表查询_value条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("NAME = 1");

        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.GROUP_CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        IDataNodeExecutor dne = ((IMerge) qc).getSubNodes().get(0);
        Assert.assertTrue(dne instanceof IJoin);
        IJoin join = (IJoin) dne;
        IQuery left = (IQuery) join.getLeftNode();
        Assert.assertEquals("TABLE1._NAME.NAME = 1", left.getKeyFilter().toString());
    }

    // 单表非主键无索引查询
    // SCHOOL上不存在索引
    // 所以会执行全表扫描
    // 只会生成一个IQuery
    // SCHOOL=1作为valueFilter
    @Test
    public void test_单表查询_非任何索引条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("SCHOOL = 1");

        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.GROUP_CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        IDataNodeExecutor dne = ((IMerge) qc).getSubNodes().get(0);
        Assert.assertTrue(dne instanceof IQuery);
        IQuery query = (IQuery) dne;
        Assert.assertEquals("TABLE1.SCHOOL = 1", query.getValueFilter().toString());
    }

    // 单表or查询
    // 查询条件由or连接，
    // 由于NAME和ID上存在索引，所以会生成两个子查询
    // or的两边分别作为子查询的keyFilter
    // 由于NAME=2323的子查询为非主键索引查询
    // 所以此处会生成一个join节点
    // 最后一个merge节点用于合并子查询的结果
    @Test
    public void test_单表查询_OR条件_1() {
        TableNode table = new TableNode("TABLE1");
        table.query("NAME = 2323 OR ID=1");
        extraCmd.put(ConnectionProperties.CHOOSE_INDEX_MERGE, true);
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        extraCmd.put(ConnectionProperties.CHOOSE_INDEX_MERGE, false);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).isUnion());// 是union查询
        Assert.assertTrue(((IMerge) qc).getSubNodes().get(0) instanceof IQuery);
        IQuery query = (IQuery) ((IMerge) qc).getSubNodes().get(0);
        Assert.assertEquals("TABLE1.ID = 1", query.getKeyFilter().toString());
        Assert.assertTrue(((IMerge) qc).getSubNodes().get(1) instanceof IMerge);
        Assert.assertTrue(((IMerge) ((IMerge) qc).getSubNodes().get(1)).getSubNodes().get(0) instanceof IJoin);
        IJoin join = (IJoin) ((IMerge) ((IMerge) qc).getSubNodes().get(1)).getSubNodes().get(0);
        Assert.assertEquals("TABLE1._NAME.NAME = 2323", ((IQuery) join.getLeftNode()).getKeyFilter().toString());
    }

    // 单表复杂查询条件
    // SCHOOL=1 AND (ID=4 OR ID=3)
    // 应该展开为
    // (SCHOOL=1 AND ID=4) OR (SCHOOL=1 AND ID=3)
    @Test
    public void test_单表查询_复杂条件展开() {
        TableNode table = new TableNode("TABLE1");
        table.query("SCHOOL=1 AND (ID=4 OR ID=3)");
        extraCmd.put(ConnectionProperties.CHOOSE_INDEX_MERGE, true);
        extraCmd.put(ConnectionProperties.EXPAND_OR, true);
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        extraCmd.put(ConnectionProperties.CHOOSE_INDEX_MERGE, false);
        extraCmd.put(ConnectionProperties.EXPAND_OR, false);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).isUnion());// 是union查询
        Assert.assertTrue(((IMerge) qc).getSubNodes().get(0) instanceof IQuery);
        Assert.assertTrue(((IMerge) qc).getSubNodes().get(1) instanceof IQuery);
        IQuery query1 = (IQuery) ((IMerge) qc).getSubNodes().get(0);
        Assert.assertEquals("TABLE1.ID = 4", query1.getKeyFilter().toString());
        Assert.assertEquals("TABLE1.SCHOOL = 1", query1.getValueFilter().toString());
        IQuery query2 = (IQuery) ((IMerge) qc).getSubNodes().get(1);
        Assert.assertEquals("TABLE1.ID = 3", query2.getKeyFilter().toString());
        Assert.assertEquals("TABLE1.SCHOOL = 1", query2.getValueFilter().toString());
    }

    // 单表复杂查询条件
    // SCHOOL=1 AND (ID=4 OR ID=3)
    // 应该展开为
    // (SCHOOL=1 AND ID=4) OR (SCHOOL=1 AND ID=3)
    @Test
    public void test_单表查询_复杂条件IN展开() {
        TableNode table = new TableNode("TABLE1");
        table.query("SCHOOL=1 AND (ID IN (4,3))");
        extraCmd.put(ConnectionProperties.CHOOSE_INDEX_MERGE, true);
        extraCmd.put(ConnectionProperties.EXPAND_IN, true);
        extraCmd.put(ConnectionProperties.EXPAND_OR, true);
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        extraCmd.put(ConnectionProperties.CHOOSE_INDEX_MERGE, false);
        extraCmd.put(ConnectionProperties.EXPAND_IN, false);
        extraCmd.put(ConnectionProperties.EXPAND_OR, false);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).isUnion());// 是union查询
        Assert.assertTrue(((IMerge) qc).getSubNodes().get(0) instanceof IQuery);
        Assert.assertTrue(((IMerge) qc).getSubNodes().get(1) instanceof IQuery);
        IQuery query1 = (IQuery) ((IMerge) qc).getSubNodes().get(0);
        Assert.assertEquals("TABLE1.ID = 4", query1.getKeyFilter().toString());
        Assert.assertEquals("TABLE1.SCHOOL = 1", query1.getValueFilter().toString());
        IQuery query2 = (IQuery) ((IMerge) qc).getSubNodes().get(1);
        Assert.assertEquals("TABLE1.ID = 3", query2.getKeyFilter().toString());
        Assert.assertEquals("TABLE1.SCHOOL = 1", query2.getValueFilter().toString());
    }

    // 两表Join查询，右表连接键为主键，右表为主键查询
    // 开启了join merge join
    // 右表为主键查询的情况下，Join策略应该选择IndexNestLoop
    @Test
    public void test_两表Join_主键() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "ID", "ID");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        // 应该是join merge join
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).getSubNodes().get(0) instanceof IJoin);
        IJoin subJoin = (IJoin) ((IMerge) qc).getSubNodes().get(0);
        Assert.assertEquals(JoinStrategy.INDEX_NEST_LOOP, subJoin.getJoinStrategy());
    }

    // 两表Join查询，右表连接键为主键，右表为主键查询
    // 开启了join merge join
    // 右表虽然为二级索引的查询，但Join列不是索引列，应该选择NestLoop
    // 会是一个table1 join ( table2 index join table2 key )的多级join
    @Test
    public void test_两表Join_主键_存在二级索引条件() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "ID", "ID");
        join.query("TABLE2.NAME = 1");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).getSubNodes().get(0) instanceof IJoin);
        IJoin subJoin = (IJoin) ((IMerge) qc).getSubNodes().get(0);
        Assert.assertTrue(subJoin.getRightNode() instanceof IJoin);
        Assert.assertEquals(JoinStrategy.NEST_LOOP_JOIN, subJoin.getJoinStrategy());
    }

    // 两表Join查询，右表连接键为主键，右表为主键查询
    // 开启了join merge join
    // 右表主键索引的查询，Join列也索引列，应该选择IndexNestLoop
    // 会是一个(table1 join table2 index ) join table2 key 的多级join
    @Test
    public void test_两表Join_主键索引_存在主键索引条件() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "ID", "ID");
        join.query("TABLE2.ID IN (1,2)");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).getSubNodes().get(0) instanceof IJoin);
        IJoin subJoin = (IJoin) ((IMerge) qc).getSubNodes().get(0);
        Assert.assertEquals(JoinStrategy.INDEX_NEST_LOOP, subJoin.getJoinStrategy());
    }

    // 两表Join查询，右表连接键为主键，右表为二级索引查询
    // 开启了join merge join
    // 右表二级索引的查询，Join列也是二级索引索引，应该选择NestLoop
    // 会是一个(table1 index join table1 index ) join (table2 index join table2
    // key)的多级join
    @Test
    public void test_两表Join_二级索引_存在二级索引条件() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "NAME", "NAME");
        join.query("TABLE2.NAME = 1");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IJoin);
        Assert.assertTrue(((IJoin) qc).getLeftNode() instanceof IJoin);
        Assert.assertTrue(((IJoin) qc).getRightNode() instanceof IMerge);
        Assert.assertEquals(JoinStrategy.INDEX_NEST_LOOP, ((IJoin) qc).getJoinStrategy());
        IJoin subJoin = (IJoin) ((IJoin) qc).getLeftNode();
        Assert.assertTrue(((IJoin) subJoin).getLeftNode() instanceof IMerge);
        Assert.assertTrue(((IJoin) subJoin).getRightNode() instanceof IMerge);
        Assert.assertEquals(JoinStrategy.INDEX_NEST_LOOP, subJoin.getJoinStrategy());
    }

    @Test
    public void test_三表Join_主键索引_存在主键索引条件() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "TABLE1.ID", "TABLE2.ID");
        join = join.join("TABLE3", "TABLE1.ID", "TABLE3.ID");
        join.query("TABLE3.ID IN (1,2)");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertTrue(((IMerge) qc).getSubNodes().get(0) instanceof IJoin);
        IJoin subJoin = (IJoin) ((IMerge) qc).getSubNodes().get(0);
        Assert.assertTrue(subJoin.getLeftNode() instanceof IJoin);
        Assert.assertEquals(JoinStrategy.INDEX_NEST_LOOP, subJoin.getJoinStrategy());
    }

    @Test
    public void test_两表join_orderby_groupby_limit条件() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "ID", "ID");
        join.select(OptimizerUtils.createColumnFromString("TABLE1.ID AS JID"),
            OptimizerUtils.createColumnFromString("CONCAT(TABLE1.NAME,TABLE1.SCHOOL) AS JNAME"));
        join.orderBy("JID");
        join.groupBy("JNAME");
        join.having("COUNT(JID) > 0");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 串行
    }

    @Test
    public void test_两表join_单独limit条件_不做并行() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE2", "ID", "ID");
        join.select(OptimizerUtils.createColumnFromString("TABLE1.ID AS JID"),
            OptimizerUtils.createColumnFromString("CONCAT(TABLE1.NAME,TABLE1.SCHOOL) AS JNAME"));
        join.limit(10, 20);
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.SEQUENTIAL, ((IMerge) qc).getQueryConcurrency());// 串行
        IJoin jn = (IJoin) ((IMerge) qc).getSubNodes().get(0);
        Assert.assertEquals("0", jn.getLimitFrom().toString());
        Assert.assertEquals("30", jn.getLimitTo().toString());
    }

    @Test
    public void test_单表查询_存在聚合函数_limit不下推() {
        TableNode table = new TableNode("TABLE1");
        table.limit(10, 20);
        table.select("count(distinct id)");
        QueryTreeNode qn = table;
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(qn, null, extraCmd);

        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.GROUP_CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        Assert.assertEquals(10L, qc.getLimitFrom());
        Assert.assertEquals(20L, qc.getLimitTo());
        IDataNodeExecutor dne = ((IMerge) qc).getSubNodes().get(0);
        Assert.assertTrue(dne instanceof IQuery);
        IQuery query = (IQuery) dne;
        Assert.assertEquals(null, query.getLimitFrom());
        Assert.assertEquals(null, query.getLimitTo());
    }

    @Test
    public void test_两表join_单库多表_单表_生成JoinMergeJoin() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE8", "ID", "ID");
        join.query("TABLE1.ID IN (0,1)");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.GROUP_CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 串行
    }

    @Test
    public void test_两表join_单表_单库多表_生成JoinMergeJoin() {
        TableNode table = new TableNode("TABLE8");
        JoinNode join = table.join("TABLE1", "ID", "ID");
        join.query("TABLE1.ID IN (0,1)");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.GROUP_CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 串行
    }

    @Test
    public void test_两表join_单表子查询_单库多表_生成JoinMergeJoin() {
        TableNode table = new TableNode("TABLE8");
        QueryNode query = new QueryNode(table);
        JoinNode join = query.join("TABLE1", "ID", "ID");
        join.query("TABLE1.ID IN (0,1)");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.GROUP_CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 串行
    }

    @Test
    public void test_两表join_单表子查询_单库多表_单表存在limit_生成JoinMergeJoin() {
        TableNode table = new TableNode("TABLE8");
        QueryNode query = new QueryNode(table);
        query.limit(1, 10);
        JoinNode join = query.join("TABLE1", "ID", "ID");
        join.query("TABLE1.ID IN (0,1)");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.GROUP_CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 串行
    }

    @Test
    public void test_两表join_多库单表_单库多表_直接是join() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE8", "ID", "ID");
        join.query("TABLE1.ID IN (1,2)");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IJoin);
        Assert.assertEquals(QUERY_CONCURRENCY.SEQUENTIAL, ((IJoin) qc).getQueryConcurrency());// 串行
    }

    @Test
    public void test_两表join_单库单表_单表会优化为下推() {
        TableNode table = new TableNode("TABLE1");
        JoinNode join = table.join("TABLE8", "NAME", "NAME");
        join.query("TABLE1.ID = 0");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(join, null, extraCmd);
        Assert.assertTrue(qc instanceof IJoin);
        Assert.assertEquals(QUERY_CONCURRENCY.SEQUENTIAL, ((IJoin) qc).getQueryConcurrency());// 串行
    }

    // @Test
    public void test_单表查询_函数下推() {
        TableNode table = new TableNode("TABLE1");
        table.select("ID");
        table.orderBy("COUNT(ID)");
        table.groupBy("NAME");
        table.having("COUNT(ID) > 1");
        table.query("ID = 1");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals(QUERY_CONCURRENCY.SEQUENTIAL, ((IQuery) qc).getQueryConcurrency());// 并行
    }

    // @Test
    public void test_单表merge_函数下推() {
        TableNode table = new TableNode("TABLE1");
        table.select("MAX(ID) AS ID");
        table.orderBy("COUNT(ID)");
        table.groupBy("SUBSTRING(NAME,0,10)");
        table.having("COUNT(ID) > 1");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, extraCmd);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行

        IDataNodeExecutor dne = ((IMerge) qc).getSubNodes().get(0);
        Assert.assertTrue(dne instanceof IQuery);
        IQuery query = (IQuery) dne;
        Assert.assertEquals("SUBSTRING(NAME, 0, 10)", query.getColumns().get(1).toString());// 下推成功
        Assert.assertEquals("COUNT(ID)", query.getColumns().get(2).toString());// 下推成功
    }

    @Test
    public void test_单表sql() {
        String sql = "select last_insert_id()";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);

        System.out.println(qc);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertTrue(qc.getSql() != null);
    }

    @Test
    public void test_group分库键_没有order() {
        String sql = "select * from TABLE1 GROUP BY ID";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.GROUP_CONCURRENT, ((IMerge) qc).getQueryConcurrency());// group并行
        Assert.assertTrue(((IMerge) qc).getOrderBys().size() == 0);
    }

    @Test
    public void test_group分库键_带order() {
        String sql = "select * from TABLE1 GROUP BY ID ORDER BY ID";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// group并行
        Assert.assertTrue(((IMerge) qc).getOrderBys().size() > 0);
    }

    @Test
    public void test_group普通字段() {
        String sql = "select * from TABLE1 GROUP BY NAME";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// group并行
        Assert.assertTrue(((QueryTree) ((IMerge) qc).getSubNode()).getOrderBys().size() > 0);
    }

    @Test
    public void test_distinct分库键() {
        String sql = "select distinct(id) from TABLE1";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.GROUP_CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        Assert.assertTrue(((IMerge) qc).getOrderBys().size() == 0);
    }

    @Test
    public void test_count_distinct分库键() {
        String sql = "select count(distinct id) from TABLE1";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.GROUP_CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        Assert.assertTrue(((IMerge) qc).getOrderBys().size() == 0);
    }

    @Test
    public void test_count_distinct普通字段() {
        String sql = "select count(distinct name) from TABLE1";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        Assert.assertTrue(((IMerge) qc).getOrderBys().size() > 0);
    }

    @Test
    public void test_distinct普通字段() {
        String sql = "select distinct(name) from TABLE1";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        Assert.assertTrue(((IMerge) qc).getOrderBys().size() > 0);
    }

    @Test
    public void test_distinct分库键_group分库键() {
        // 这种sql group没意义，distinct的优先级高于group
        String sql = "select distinct(id) from TABLE1 group by id";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        Assert.assertTrue(((IMerge) qc).getOrderBys().size() > 0);
    }

    @Test
    public void test_distinct分库键_存在orderby() {
        String sql = "select distinct(id) from TABLE1 order by id";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
        Assert.assertTrue(((IMerge) qc).getOrderBys().size() > 0);
    }

    @Test
    public void test_存在条件_存在orderby() {
        String sql = "select * from TABLE1 where id > 1 order by id";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(QUERY_CONCURRENCY.CONCURRENT, ((IMerge) qc).getQueryConcurrency());// 并行
    }

    @Test
    public void test_单表_forUpdate() {
        String sql = "select * from TABLE1 where id = 1 for update";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals(LOCK_MODE.EXCLUSIVE_LOCK, ((IQuery) qc).getLockMode());
    }

    @Test
    public void test_单表_shareLock() {
        String sql = "select * from TABLE1 where id = 1 LOCK IN SHARE MODE";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals(LOCK_MODE.SHARED_LOCK, ((IQuery) qc).getLockMode());
    }

    @Test
    public void test_merge_forUpdate() {
        String sql = "select * from TABLE1 where id in (1,2,3) for update";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(LOCK_MODE.EXCLUSIVE_LOCK, ((IQuery) ((IMerge) qc).getSubNode()).getLockMode());
    }

    @Test
    public void test_子查询_内部forUpdate() {
        String sql = "select * from (select * from TABLE1 where id = 1 for update) a";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals(LOCK_MODE.EXCLUSIVE_LOCK, ((IQuery) ((IQuery) qc).getSubQuery()).getLockMode());
    }

    @Test
    public void test_子查询_外部forUpdate() {
        String sql = "select * from (select * from TABLE1 where id = 1) a for update";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals(LOCK_MODE.EXCLUSIVE_LOCK, ((IQuery) qc).getLockMode());
    }

    @Test
    public void testQuery_子查询_in模式() throws SqlParserException {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE NAME IN (SELECT NAME FROM TABLE2 WHERE TABLE2.NAME = TABLE1.NAME) AND ID IN (1,2)";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);

        IQuery query1 = (IQuery) ((IMerge) qc).getSubNodes().get(0);
        IQuery query2 = (IQuery) ((IMerge) qc).getSubNodes().get(1);

        IFunction subquery1 = (IFunction) ((List) query1.getSubqueryFilter().getArgs().get(1)).get(0);
        IFunction subquery2 = (IFunction) ((List) query2.getSubqueryFilter().getArgs().get(1)).get(0);
        Assert.assertTrue(subquery1.getArgs().get(0) instanceof IQuery);

        IQuery merge1 = (IQuery) subquery1.getArgs().get(0);
        IQuery merge2 = (IQuery) subquery2.getArgs().get(0);
        Assert.assertEquals(merge1.getSubqueryOnFilterId(), merge2.getSubqueryOnFilterId());
    }

    @Test
    public void testQuery_子查询_多字段in模式() throws SqlParserException {
        String sql = "SELECT * FROM TABLE1 WHERE (ID,SCHOOL) IN (SELECT ID,SCHOOL FROM TABLE2 WHERE TABLE2.NAME = TABLE1.NAME)";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);

        IQuery query1 = (IQuery) ((IMerge) qc).getSubNodes().get(0);
        IQuery query2 = (IQuery) ((IMerge) qc).getSubNodes().get(1);

        IFunction subquery1 = (IFunction) ((List) query1.getSubqueryFilter().getArgs().get(1)).get(0);
        IFunction subquery2 = (IFunction) ((List) query2.getSubqueryFilter().getArgs().get(1)).get(0);
        Assert.assertTrue(subquery1 == subquery2);
        Assert.assertTrue((IJoin) subquery1.getArgs().get(0) instanceof IJoin);
    }

    @Test
    public void testQuery_子查询_not_in模式() throws SqlParserException {
        String sql = "SELECT * FROM TABLE1 WHERE ID NOT IN (SELECT ID FROM TABLE2 WHERE TABLE2.NAME = TABLE1.NAME)";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);

        IQuery query1 = (IQuery) ((IMerge) qc).getSubNodes().get(0);
        IQuery query2 = (IQuery) ((IMerge) qc).getSubNodes().get(1);

        IFunction subquery1 = (IFunction) ((List) query1.getSubqueryFilter().getArgs().get(1)).get(0);
        IFunction subquery2 = (IFunction) ((List) query2.getSubqueryFilter().getArgs().get(1)).get(0);
        Assert.assertTrue(subquery1 == subquery2);
        Assert.assertTrue((IQuery) subquery1.getArgs().get(0) instanceof IQuery);
    }

    @Test
    public void testQuery_子查询correlated_in模式() throws SqlParserException {
        String sql = "SELECT * FROM TABLE1 WHERE ID IN (SELECT ID FROM TABLE2 WHERE TABLE2.NAME = TABLE1.NAME)";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);

        IQuery query1 = (IQuery) ((IMerge) qc).getSubNodes().get(0);
        IQuery query2 = (IQuery) ((IMerge) qc).getSubNodes().get(1);

        IFunction subquery1 = (IFunction) ((List) query1.getSubqueryFilter().getArgs().get(1)).get(0);
        IFunction subquery2 = (IFunction) ((List) query2.getSubqueryFilter().getArgs().get(1)).get(0);
        Assert.assertTrue(subquery1 == subquery2);
        Assert.assertTrue(subquery1.getArgs().get(0) instanceof IQuery);
    }

    @Test
    public void testQuery_子查询_exist模式() throws SqlParserException {
        String sql = "SELECT * FROM TABLE1 WHERE  EXISTS (SELECT ID FROM TABLE2 WHERE TABLE2.NAME = TABLE1.NAME)";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);

        IQuery query1 = (IQuery) ((IMerge) qc).getSubNodes().get(0);
        IQuery query2 = (IQuery) ((IMerge) qc).getSubNodes().get(1);

        IFunction subquery1 = (IFunction) query1.getSubqueryFilter().getArgs().get(0);
        IFunction subquery2 = (IFunction) query2.getSubqueryFilter().getArgs().get(0);
        Assert.assertTrue(subquery1 == subquery2);
        Assert.assertTrue((IQuery) subquery1.getArgs().get(0) instanceof IQuery);
    }

    @Test
    public void testQuery_子查询_not_exist模式() throws SqlParserException {
        String sql = "SELECT * FROM TABLE1 WHERE NOT EXISTS (SELECT ID FROM TABLE2 WHERE TABLE2.NAME = TABLE1.NAME)";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);

        IQuery query1 = (IQuery) ((IMerge) qc).getSubNodes().get(0);
        IQuery query2 = (IQuery) ((IMerge) qc).getSubNodes().get(1);

        IFunction subquery1 = (IFunction) query1.getSubqueryFilter().getArgs().get(0);
        IFunction subquery2 = (IFunction) query2.getSubqueryFilter().getArgs().get(0);
        Assert.assertTrue(subquery1 == subquery2);
        Assert.assertEquals("NOT", subquery1.getFunctionName());
        // 结构为： NOT(func) -> FILTER -> SUBQUERY_SCALAR(func) -> subquery
        Assert.assertTrue((IQuery) ((IFunction) ((IBooleanFilter) subquery1.getArgs().get(0)).getArgs().get(0)).getArgs()
            .get(0) instanceof IQuery);
    }

    @Test
    public void testQuery_子查询_all模式() throws SqlParserException {
        String sql = "SELECT * FROM TABLE1 WHERE ID > ALL (SELECT ID FROM TABLE2 WHERE TABLE2.NAME = TABLE1.NAME)";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);

        IQuery query1 = (IQuery) ((IMerge) qc).getSubNodes().get(0);
        IQuery query2 = (IQuery) ((IMerge) qc).getSubNodes().get(1);

        IFunction subquery1 = (IFunction) query1.getSubqueryFilter().getArgs().get(1);
        IFunction subquery2 = (IFunction) query2.getSubqueryFilter().getArgs().get(1);
        Assert.assertTrue(subquery1 == subquery2);
        Assert.assertTrue(subquery1.getArgs().get(0) instanceof IQuery);
        IQuery merge = (IQuery) subquery1.getArgs().get(0);
        IFunction func = (IFunction) merge.getColumns().get(0);
        Assert.assertEquals("MAX", func.getFunctionName());
    }

    @Test
    public void testQuery_子查询_any模式() throws SqlParserException {
        // SubqueryAnyExpression
        String sql = "SELECT * FROM TABLE1 WHERE ID <= ANY (SELECT ID FROM TABLE2 WHERE TABLE2.NAME = TABLE1.NAME)";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);

        IQuery query1 = (IQuery) ((IMerge) qc).getSubNodes().get(0);
        IQuery query2 = (IQuery) ((IMerge) qc).getSubNodes().get(1);

        IFunction subquery1 = (IFunction) query1.getSubqueryFilter().getArgs().get(1);
        IFunction subquery2 = (IFunction) query2.getSubqueryFilter().getArgs().get(1);
        Assert.assertTrue(subquery1 == subquery2);
        Assert.assertTrue(subquery1.getArgs().get(0) instanceof IQuery);
        IQuery merge = (IQuery) subquery1.getArgs().get(0);
        IFunction func = (IFunction) merge.getColumns().get(0);
        Assert.assertEquals("MAX", func.getFunctionName());
    }

    @Test
    public void test_lastInsertId() throws SqlParserException {
        String sql = "SELECT LAST_INSERT_ID()";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc.getSql() != null);
        Assert.assertEquals(IDataNodeExecutor.USE_LAST_DATA_NODE, qc.getDataNode());
    }

    @Test
    public void test_nextval() throws SqlParserException {
        String sql = "SELECT TABLE1.NEXTVAL,TABLE1.NEXTVAL,TABLE1.NEXTVAL FROM DUAL";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc.getSql() == null);
        Assert.assertEquals("[1, 2, 3]", qc.getColumns().toString());
        Assert.assertEquals(Long.valueOf(3), qc.getLastSequenceVal());

        sql = "SELECT * FROM TABLE1 WHERE (ID > 10 OR ID < 5) OR ID IN (TABLE1.NEXTVAL)";
        qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc.getSql() == null);
        Assert.assertEquals(Long.valueOf(4), qc.getLastSequenceVal());
    }

    @Test
    public void test_batchNextVal() throws SqlParserException {
        String sql = "INSERT INTO TABLE1(ID,NAME,SCHOOL) VALUES(TABLE1.NEXTVAL,?,?)";

        Parameters parameterSettings = new Parameters();
        for (int i = 0; i < 10; i++) {
            ParameterContext p1 = new ParameterContext(ParameterMethod.setString, new Object[] { 1, "ljh" + i });
            ParameterContext p2 = new ParameterContext(ParameterMethod.setString, new Object[] { 2, "school" + i });
            parameterSettings.getCurrentParameter().put(1, p1);
            parameterSettings.getCurrentParameter().put(2, p2);

            parameterSettings.addBatch();
        }

        IMerge merge = (IMerge) optimizer.optimizeAndAssignment(sql, parameterSettings, extraCmd, true);
        System.out.println(merge);
    }

    @Test
    public void test_insertSelect_都是分表() {
        String sql = "INSERT INTO TABLE1 SELECT * FROM TABLE2";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(8, ((IMerge) qc).getSubNodes().size());
    }

    @Test
    public void test_insertSelect_都是分表_带where条件() {
        String sql = "INSERT INTO TABLE1(ID,NAME) SELECT ID,NAME FROM TABLE2 WHERE ID IN (1,2)";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(2, ((IMerge) qc).getSubNodes().size());
    }

    @Test
    public void test_insertSelect_都是广播表() {
        String sql = "INSERT INTO TABLE7 SELECT * FROM TABLE7";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(4, ((IMerge) qc).getSubNodes().size());
    }
}
