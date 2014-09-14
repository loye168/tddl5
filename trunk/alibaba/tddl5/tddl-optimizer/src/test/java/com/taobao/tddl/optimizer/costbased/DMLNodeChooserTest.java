package com.taobao.tddl.optimizer.costbased;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.dml.IInsert;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;

/**
 * @author Dreamond
 */
public class DMLNodeChooserTest extends BaseOptimizerTest {

    @Test
    public void testUpdate() {
        TableNode table = new TableNode("TABLE1");
        String values[] = { "NAME" };
        UpdateNode update = ((TableNode) table.query("ID>=5 AND ID<=100")).update("NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(update, null, null);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(8, ((IMerge) plan).getSubNodes().size());

        String sql = "UPDATE TABLE1 SET NAME = NAME WHERE ID>=5 AND ID<=100";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(8, ((IMerge) plan).getSubNodes().size());
    }

    public void testUpdate_范围更新生成merge() {
        TableNode table = new TableNode("TABLE1");
        String values[] = { "NAME" };
        UpdateNode update = ((TableNode) table.query("ID>=5")).update("NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(update, null, null);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(8, ((IMerge) plan).getSubNodes().size());

        String sql = "UPDATE TABLE1 SET NAME = NAME WHERE ID>=5";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(8, ((IMerge) plan).getSubNodes().size());
    }

    @Test
    public void testUpdate_广播表() {
        TableNode table = new TableNode("TABLE7");
        String values[] = { "NAME" };
        UpdateNode update = ((TableNode) table.query("ID>=5 AND ID<=100")).update("NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(update, null, null);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(4, ((IMerge) plan).getSubNodes().size());

        String sql = "UPDATE TABLE7 SET NAME = NAME WHERE ID>=5 AND ID<=100";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(4, ((IMerge) plan).getSubNodes().size());
    }

    @Test
    public void testPut() {
        TableNode table = new TableNode("TABLE1");
        Comparable values[] = { 2 };
        PutNode update = ((TableNode) table).put("ID", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(update, null, null);
        Assert.assertTrue(plan instanceof IPut);

        String sql = "INSERT INTO TABLE1(ID) VALUES(2)";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IPut);
    }

    @Test
    public void testPut_多value() {
        String sql = "REPLACE INTO TABLE1(ID) VALUES(1),(2),(3),(4),(5),(6),(7),(8)";
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(8, ((IMerge) plan).getSubNodes().size());
    }

    @Test
    public void testPut_广播表() {
        TableNode table = new TableNode("TABLE7");
        Comparable values[] = { 2 };
        PutNode update = ((TableNode) table).put("ID", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(update, null, null);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(4, ((IMerge) plan).getSubNodes().size());

        String sql = "REPLACE INTO TABLE7(ID) VALUES(2)";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(4, ((IMerge) plan).getSubNodes().size());
    }

    @Test
    public void testPut_多value_广播表() {
        String sql = "REPLACE INTO TABLE7(ID) VALUES(1),(2),(3),(4),(5),(6),(7),(8)";
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(4, ((IMerge) plan).getSubNodes().size());
    }

    @Test
    public void testPut_全字段() {
        TableNode table = new TableNode("TABLE1");
        Comparable values[] = { 2, "sysu", "sun" };
        PutNode put = table.put("ID SCHOOL NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(put, null, null);
        Assert.assertTrue(plan instanceof IPut);

        String sql = "REPLACE INTO TABLE1(ID,SCHOOL,NAME) VALUES(2,'sysu','sun')";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IPut);
    }

    @Test
    public void testPut_自增id_null值() {
        TableNode table = new TableNode("AUTOINC");
        Comparable values[] = { null, "sysu", "sun" };
        PutNode put = table.put("ID SCHOOL NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(put, null, null);
        Assert.assertTrue(plan instanceof IPut);

        String sql = "REPLACE INTO AUTOINC(ID,SCHOOL,NAME) VALUES(null,'sysu','sun')";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IPut);
    }

    @Test
    public void testPut_自增id_不存在列() {
        TableNode table = new TableNode("AUTOINC");
        Comparable values[] = { "sysu", "sun" };
        PutNode put = table.put("SCHOOL NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(put, null, null);
        Assert.assertTrue(plan instanceof IPut);

        String sql = "REPLACE INTO AUTOINC(SCHOOL,NAME) VALUES('sysu','sun')";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IPut);
    }

    @Test
    public void testDelete() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID>=5 AND ID<=100");
        ASTNode delete = ((TableNode) table).delete();// .delete();
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(delete, null, null);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(8, ((IMerge) plan).getSubNodes().size());

        String sql = "DELETE FROM TABLE1 WHERE ID>=5 AND ID<=100";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(8, ((IMerge) plan).getSubNodes().size());
    }

    @Test
    public void testDelete_广播表() {
        TableNode table = new TableNode("TABLE7");
        table.query("ID>=5 AND ID<=100");
        ASTNode delete = ((TableNode) table).delete();// .delete();
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(delete, null, null);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(4, ((IMerge) plan).getSubNodes().size());

        String sql = "DELETE FROM TABLE7 WHERE ID>=5 AND ID<=100";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(4, ((IMerge) plan).getSubNodes().size());
    }

    @Test
    public void testDelete_范围删除会生成merge() {
        TableNode table = new TableNode("TABLE1");
        DeleteNode delete = ((TableNode) table.query("ID>=5")).delete();
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(delete, null, null);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(8, ((IMerge) plan).getSubNodes().size());

        String sql = "DELETE FROM TABLE1 WHERE ID>=5";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(8, ((IMerge) plan).getSubNodes().size());
    }

    @Test
    public void testInsert() {
        TableNode table = new TableNode("TABLE1");
        Comparable values[] = { 2 };
        InsertNode insert = table.insert("ID", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(insert, null, null);
        Assert.assertTrue(plan instanceof IInsert);

        String sql = "INSERT INTO TABLE1(ID) VALUES(2)";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IInsert);
    }

    @Test
    public void testInsert_多value() {
        String sql = "INSERT INTO TABLE1(ID) VALUES(1),(2),(3),(4),(5),(6),(7),(8)";
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(8, ((IMerge) plan).getSubNodes().size());
    }

    @Test
    public void testInsert_广播表() {
        TableNode table = new TableNode("TABLE7");
        Comparable values[] = { 2 };
        InsertNode insert = table.insert("ID", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(insert, null, null);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(4, ((IMerge) plan).getSubNodes().size());

        String sql = "INSERT INTO TABLE7(ID) VALUES(2)";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(4, ((IMerge) plan).getSubNodes().size());
    }

    @Test
    public void testInsert_多value_广播表() {
        String sql = "INSERT INTO TABLE7(ID) VALUES(1),(2),(3),(4),(5),(6),(7),(8)";
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(4, ((IMerge) plan).getSubNodes().size());
    }

    @Test
    public void testInsert_全字段() {
        TableNode table = new TableNode("TABLE1");
        Comparable values[] = { 2, "sysu", "sun" };
        InsertNode insert = table.insert("ID SCHOOL NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(insert, null, null);
        Assert.assertTrue(plan instanceof IInsert);

        String sql = "INSERT INTO TABLE1 VALUES(2,'sysu','sun')";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IInsert);
    }

    @Test
    public void testInsert_自增id_null值() {
        TableNode table = new TableNode("AUTOINC");
        Comparable values[] = { null, "sysu", "sun" };
        InsertNode insert = table.insert("ID SCHOOL NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(insert, null, null);
        Assert.assertTrue(plan instanceof IInsert);

        String sql = "INSERT INTO AUTOINC(ID,SCHOOL,NAME) VALUES(null,'sysu','sun')";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IInsert);
    }

    @Test
    public void testInsert_自增id_不存在列() {
        TableNode table = new TableNode("AUTOINC");
        Comparable values[] = { "sysu", "sun" };
        InsertNode insert = table.insert("SCHOOL NAME", values);
        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(insert, null, null);
        Assert.assertTrue(plan instanceof IInsert);

        String sql = "INSERT INTO AUTOINC(SCHOOL,NAME) VALUES('sysu','sun')";
        plan = optimizer.optimizeAndAssignment(sql, null, null, false);
        Assert.assertTrue(plan instanceof IInsert);
    }

}
