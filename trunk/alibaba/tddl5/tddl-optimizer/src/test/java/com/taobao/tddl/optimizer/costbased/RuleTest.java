package com.taobao.tddl.optimizer.costbased;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.exception.SqlParserException;

public class RuleTest extends BaseOptimizerTest {

    @Test
    public void testQueryNoCondition() throws SqlParserException {
        String sql = "SELECT S.ID AS ID1 , T.ID AS ID2 FROM STUDENT S JOIN STUDENT T ON S.ID=T.ID AND S.ID= ? AND T.SCHOOL= ?";
        // extraCmd.put(ExtraCmd.JoinMergeJoin, "true");
        IDataNodeExecutor qc1 = optimizer.optimizeAndAssignment(sql, convert(new Integer[] { 1, 3 }), null, false);
        Assert.assertEquals(qc1.getSql(), sql);
    }

    @Test
    public void testSinleDb() {
        String sql = "SELECT * FROM STUDENT WHERE ID = ?";
        IDataNodeExecutor qc1 = optimizer.optimizeAndAssignment(sql, convert(new Integer[] { 1, 3 }), null, false);
        Assert.assertEquals(qc1.getSql(), sql);
    }

    @Test
    public void test一张表为单库_另一张表为分库() {
        String sql = "SELECT * FROM STUDENT s , TABLE1 t WHERE s.ID = ? AND s.ID = t.ID";
        IDataNodeExecutor qc1 = optimizer.optimizeAndAssignment(sql, convert(new Integer[] { 1, 3 }), null, false);
        Assert.assertEquals(qc1.getSql(), null);
    }

    @Test
    public void test_字段为null值() {
        String sql = "SELECT * FROM TABLE1 WHERE ID = ?";
        IDataNodeExecutor qc1 = optimizer.optimizeAndAssignment(sql, convert(new Object[] { null }), null, false);
        Assert.assertTrue(qc1 instanceof IMerge);
    }

}
