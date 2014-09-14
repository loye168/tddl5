package com.taobao.tddl.optimizer.costbased;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.model.SqlType;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.exception.SqlParserException;
import com.taobao.tddl.optimizer.parse.SqlAnalysisResult;

public class SubQueryPreProcessorTest extends BaseOptimizerTest {

    @Test
    public void testQuery_子查询_in模式() throws SqlParserException {
        String sql = "SELECT * FROM TABLE1 WHERE (ID > 10 OR ID < 5) OR  ID IN (SELECT ID FROM TABLE2 WHERE ID > 10 OR ID < 5)";
        QueryTreeNode qn = query(sql);
        FilterPreProcessor.optimize(qn, true, null);
        qn.build();

        IFunction func = SubQueryPreProcessor.findNextSubqueryOnFilter(qn);
        Assert.assertTrue(func != null);

        QueryTreeNode qtn = (QueryTreeNode) func.getArgs().get(0);
        Map<Long, Object> subquerySettings = new HashMap<Long, Object>();
        subquerySettings.put(qtn.getSubqueryOnFilterId(), Arrays.asList(1, 2));
        func = SubQueryPreProcessor.assignmentSubqueryOnFilter(qn, subquerySettings);
        FilterPreProcessor.optimize(qn, true, null);
        Assert.assertTrue(func == null);
    }

    @Test
    public void testQuery_子查询_correlate_in模式() throws SqlParserException {
        String sql = "SELECT * FROM TABLE1 WHERE ID IN (SELECT ID FROM TABLE2 WHERE TABLE2.NAME = TABLE1.NAME)";
        QueryTreeNode qn = query(sql);
        qn.build();

        IFunction func = SubQueryPreProcessor.findNextSubqueryOnFilter(qn);
        Assert.assertTrue(func == null);
    }

    @Test
    public void testQuery_子查询_多级查询() throws SqlParserException {
        String sql = "SELECT * FROM TABLE1 WHERE ID IN (SELECT ID FROM TABLE2 WHERE NAME = (SELECT NAME FROM TABLE3))";
        sql += " AND NAME = (SELECT NAME FROM TABLE4) AND SCHOOL > ALL (SELECT SCHOOL FROM TABLE5)";
        QueryTreeNode qn = query(sql);
        qn.build();

        List<IFunction> funcs = SubQueryPreProcessor.findAllSubqueryOnFilter(qn, true);
        Assert.assertEquals(4, funcs.size());

        IFunction func = SubQueryPreProcessor.findNextSubqueryOnFilter(qn);
        Assert.assertTrue(func != null);
        Map<Long, Object> subquerySettings = new HashMap<Long, Object>();
        subquerySettings.put(((QueryTreeNode) func.getArgs().get(0)).getSubqueryOnFilterId(), 1);

        func = SubQueryPreProcessor.assignmentSubqueryOnFilter(qn, subquerySettings);
        Assert.assertTrue(func != null);
        subquerySettings = new HashMap<Long, Object>();
        subquerySettings.put(((QueryTreeNode) func.getArgs().get(0)).getSubqueryOnFilterId(), Arrays.asList(1, 2));

        func = SubQueryPreProcessor.assignmentSubqueryOnFilter(qn, subquerySettings);
        Assert.assertTrue(func != null);
        subquerySettings = new HashMap<Long, Object>();
        subquerySettings.put(((QueryTreeNode) func.getArgs().get(0)).getSubqueryOnFilterId(), "LJH");

        func = SubQueryPreProcessor.assignmentSubqueryOnFilter(qn, subquerySettings);
        Assert.assertTrue(func != null);
        subquerySettings = new HashMap<Long, Object>();
        subquerySettings.put(((QueryTreeNode) func.getArgs().get(0)).getSubqueryOnFilterId(), 3);

        func = SubQueryPreProcessor.assignmentSubqueryOnFilter(qn, subquerySettings);
        Assert.assertTrue(func == null);
    }

    @Test
    public void testQuery_子查询_各种位置子查询() throws SqlParserException {
        String sql = "SELECT (SELECT MAX(ID) FROM TABLE1)+1 FROM TABLE1 WHERE NOT EXISTS(SELECT MAX(ID) FROM TABLE2 LIMIT 1) AND (SELECT MAX(ID) FROM TABLE3) + 1 > (SELECT ID FROM TABLE4 WHERE NAME = (SELECT NAME FROM TABLE5))";
        sql += " GROUP BY ID + (SELECT MAX(ID) FROM TABLE6) HAVING ID + (SELECT MAX(ID) FROM TABLE7) > 10 ORDER BY ID + (SELECT MAX(ID) FROM TABLE8) LIMIT 10,10";
        QueryTreeNode qn = query(sql);
        qn.build();

        List<IFunction> funcs = SubQueryPreProcessor.findAllSubqueryOnFilter(qn, true);
        Assert.assertEquals(8, funcs.size());

        IFunction func = SubQueryPreProcessor.findNextSubqueryOnFilter(qn);
        Assert.assertTrue(func != null);
        for (int i = 0; i < 7; i++) {
            Map<Long, Object> subquerySettings = new HashMap<Long, Object>();
            subquerySettings.put(((QueryTreeNode) func.getArgs().get(0)).getSubqueryOnFilterId(), 1);
            func = SubQueryPreProcessor.assignmentSubqueryOnFilter(qn, subquerySettings);
            Assert.assertTrue(func != null);
        }

        Map<Long, Object> subquerySettings = new HashMap<Long, Object>();
        subquerySettings.put(((QueryTreeNode) func.getArgs().get(0)).getSubqueryOnFilterId(), 1);
        func = SubQueryPreProcessor.assignmentSubqueryOnFilter(qn, subquerySettings);
        Assert.assertTrue(func == null);
    }

    @Test
    public void testQuery_子查询_correlate替换() throws SqlParserException {
        String sql = "SELECT * FROM TABLE1 WHERE ID IN (SELECT ID FROM TABLE2 WHERE TABLE2.NAME = TABLE1.NAME)";
        QueryTreeNode qn = query(sql);
        qn.build();

        List<IFunction> funcs = SubQueryPreProcessor.findAllSubqueryOnFilter(qn, true);
        IFunction func = funcs.get(0);
        Assert.assertTrue(func != null);

        QueryTreeNode qtn = (QueryTreeNode) func.getArgs().get(0);
        Map<Long, Object> subquerySettings = new HashMap<Long, Object>();
        subquerySettings.put(qtn.getColumnsCorrelated().get(0).getCorrelateOnFilterId(), 3);
        func = SubQueryPreProcessor.assignmentSubqueryOnFilter(qn, subquerySettings);
        Assert.assertTrue(func == null);
    }

    private QueryTreeNode query(String sql) throws SqlParserException {
        SqlAnalysisResult sm = parser.parse(sql, false);
        QueryTreeNode qn = null;
        if (sm.getSqlType() == SqlType.SELECT) {
            qn = sm.getQueryTreeNode();
        } else {
            qn = new KVIndexNode(null);
            qn.setSql(sql);
        }
        return qn;
    }
}
