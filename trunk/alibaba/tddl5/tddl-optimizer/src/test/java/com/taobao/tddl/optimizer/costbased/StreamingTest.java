package com.taobao.tddl.optimizer.costbased;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

public class StreamingTest extends BaseOptimizerTest {

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
    public void test_单表merge无条件_开启streaming() {
        String sql = "select * from TABLE1";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(true, ((IMerge) qc).isStreaming());
        Assert.assertEquals(true, ((IMerge) qc).getSubNode().isStreaming());
    }

    // @Test
    // public void test_单表merge_存在聚合函数_开启streaming() {
    // String sql = "select count(*) from TABLE1";
    // IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null,
    // extraCmd, true);
    // Assert.assertTrue(qc instanceof IMerge);
    // Assert.assertEquals(false, ((IMerge) qc).isStreaming());
    // Assert.assertEquals(false, ((IMerge) qc).getSubNode().isStreaming());
    // }

    @Test
    public void test_单表merge_limit_开启streaming() {
        String sql = "select * from TABLE1 WHERE ID > 10 LIMIT 10,91";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(false, ((IMerge) qc).isStreaming());
        Assert.assertEquals(true, ((IMerge) qc).getSubNode().isStreaming());
    }

    @Test
    public void test_单表_limit_不开启streaming() {
        String sql = "select * from TABLE1 WHERE ID = 1 LIMIT 10,91";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals(false, ((IQuery) qc).isStreaming());
    }

    @Test
    public void test_单表_无条件_开启streaming() {
        String sql = "select * from TABLE7";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals(true, ((IQuery) qc).isStreaming());
    }

    @Test
    public void test_joinMergejoin无条件_开启streaming() {
        String sql = "select * from TABLE1 inner join  TABLE2 ON TABLE1.ID = TABLE2.ID";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(true, ((IMerge) qc).isStreaming());
        Assert.assertEquals(true, ((IMerge) qc).getSubNode().isStreaming()); // 当前节点为true即可
        Assert.assertEquals(true, ((IJoin) ((IMerge) qc).getSubNode()).getLeftNode().isStreaming());
        Assert.assertEquals(false, ((IJoin) ((IMerge) qc).getSubNode()).getRightNode().isStreaming());
    }

    @Test
    public void test_joinMergejoin_limit_开启streaming() {
        String sql = "select * from TABLE1 t1 inner join TABLE2 t2 ON t1.ID = t2.ID limit 10,91";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertEquals(false, ((IMerge) qc).isStreaming());
        Assert.assertEquals(true, ((IMerge) qc).getSubNode().isStreaming());
    }

    @Test
    public void test_join_limit_不开启streaming() {
        String sql = "select * from TABLE1 t1 inner join TABLE2 t2 ON t1.ID = t2.ID where t1.id = 1 limit 10,91";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertEquals(false, ((IJoin) qc).isStreaming()); // 直接下推，当前根节点为false即可
        Assert.assertEquals(true, ((IJoin) qc).getLeftNode().isStreaming());
        Assert.assertEquals(false, ((IJoin) qc).getRightNode().isStreaming());
    }

    @Test
    public void test_join_无条件_开启streaming() {
        String sql = "select * from TABLE1 t1 inner join TABLE2 t2 ON t1.ID = t2.ID";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertEquals(true, ((IMerge) qc).isStreaming()); // 直接下推，当前根节点为false即可
        Assert.assertEquals(true, ((IMerge) qc).getSubNode().isStreaming());
    }

    @Test
    public void test_subQuery_merge无条件_开启streaming() {
        String sql = "select * from (select * from table1) a";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(true, ((IMerge) qc).isStreaming());
        Assert.assertEquals(true, ((IMerge) qc).getSubNode().isStreaming());
        Assert.assertEquals(true, ((IQuery) ((IMerge) qc).getSubNode()).getSubQuery().isStreaming());
    }

    @Test
    public void test_subQuery_merge_limit_开启streaming() {
        String sql = "select * from (select * from table1) a limit 10,91";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertEquals(false, ((IMerge) qc).isStreaming());
        Assert.assertEquals(true, ((IMerge) qc).getSubNode().isStreaming());
        Assert.assertEquals(true, ((IQuery) ((IMerge) qc).getSubNode()).getSubQuery().isStreaming());
    }

    @Test
    public void test_subQuery_limit_不开启streaming() {
        String sql = "select * from (select * from table1 where id = 1) a limit 10,91";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals(false, ((IQuery) qc).isStreaming());
        Assert.assertEquals(true, ((IQuery) qc).getSubQuery().isStreaming());
    }

    @Test
    public void test_subQuery_无条件_开启streaming() {
        String sql = "select * from (select * from table7) a";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals(true, ((IQuery) qc).isStreaming());
    }

    @Test
    public void test_subQuery_子节点merge无条件_开启streaming() {
        String sql = "select * from (select * from table1 group by id) a";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IQuery);
        // 主要存在group by聚合操作
        Assert.assertEquals(false, ((IQuery) qc).isStreaming());
        Assert.assertEquals(false, ((IQuery) qc).getSubQuery().isStreaming());
    }

    @Test
    public void test_subQuery_子节点merge_limit_开启streaming() {
        String sql = "select * from (select * from table1 group by id) a limit 10";
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals(false, ((IQuery) qc).isStreaming());
        Assert.assertEquals(true, ((IQuery) qc).getSubQuery().isStreaming());
    }

    @Test
    public void test_单表merge无条件_强制关闭streaming() {
        String sql = "select * from TABLE1";
        extraCmd.put(ConnectionProperties.CHOOSE_STREAMING, false);
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        extraCmd.remove(ConnectionProperties.CHOOSE_STREAMING);
        Assert.assertTrue(qc instanceof IMerge);
        Assert.assertEquals(false, ((IMerge) qc).isStreaming());
        Assert.assertEquals(false, ((IMerge) qc).getSubNode().isStreaming());
    }

    @Test
    public void test_单表_limit_强制开启streaming() {
        String sql = "select * from TABLE1 WHERE ID = 1 LIMIT 10,91";
        extraCmd.put(ConnectionProperties.CHOOSE_STREAMING, true);
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(sql, null, extraCmd, true);
        extraCmd.remove(ConnectionProperties.CHOOSE_STREAMING);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals(true, ((IQuery) qc).isStreaming());
    }
}
