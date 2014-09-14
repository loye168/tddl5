//package com.taobao.tddl.matrix.test;
//
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//
//import org.junit.Assert;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import com.taobao.tddl.common.exception.TddlException;
//import com.taobao.tddl.executor.common.ExecutionContext;
//import com.taobao.tddl.executor.transaction.StrictConnectionHolder;
//import com.taobao.tddl.executor.transaction.StrictlTransaction;
//import com.taobao.tddl.matrix.jdbc.TConnection;
//import com.taobao.tddl.matrix.jdbc.TDataSource;
//
//public class TransactionTest {
//
//    static TDataSource ds = new TDataSource();
//
//    @BeforeClass
//    public static void initTestWithDS() throws TddlException, SQLException {
//        ds.setAppName("andor_show");
//        ds.setRuleFile("test_rule.xml");
//        ds.setTopologyFile("test_matrix.xml");
//        ds.setSchemaFile("test_schema.xml");
//        ds.init();
//    }
//
//    @Test
//    public void testNotAutoCommit() throws Exception {
//        TConnection conn = (TConnection) ds.getConnection();
//        StrictConnectionHolder ch = conn.getConnectionHolder();
//
//        conn.setAutoCommit(false);
//        ExecutionContext context = conn.getExecutionContext();
//        Assert.assertTrue(context.getTransaction() == null);
//        {
//            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
//            ResultSet rs = ps.executeQuery();
//            rs.next();
//            rs.close();
//        }
//
//        Assert.assertTrue(context.getTransaction() != null);
//        StrictlTransaction t = (StrictlTransaction) context.getTransaction();
//
//        Assert.assertEquals("My_Transaction", t.getClass().getSimpleName());
//        Assert.assertEquals("andor_show_group1", context.getTransactionGroup());
//
//        Assert.assertEquals(1, ch.getAllConnection().size());
//
//        {
//            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
//            ResultSet rs = ps.executeQuery();
//            rs.next();
//            rs.close();
//        }
//
//        Assert.assertEquals(t, conn.getExecutionContext().getTransaction());
//        Assert.assertEquals(1, ch.getAllConnection().size());
//        conn.commit();
//        conn.close();
//
//        Assert.assertEquals(0, ch.getAllConnection().size());
//
//    }
//
//    @Test
//    public void testNotAutoCommitOnDifferentGroup() throws Exception {
//        TConnection conn = (TConnection) ds.getConnection();
//        StrictConnectionHolder ch = conn.getConnectionHolder();
//
//        conn.setAutoCommit(false);
//        ExecutionContext context = conn.getExecutionContext();
//        Assert.assertTrue(context.getTransaction() == null);
//        {
//            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
//            ResultSet rs = ps.executeQuery();
//            rs.next();
//            rs.close();
//        }
//
//        Assert.assertTrue(context.getTransaction() != null);
//        StrictlTransaction t = (StrictlTransaction) context.getTransaction();
//        Assert.assertEquals("My_Transaction", t.getClass().getSimpleName());
//        Assert.assertEquals("andor_show_group1", context.getTransactionGroup());
//        Assert.assertEquals(1, ch.getAllConnection().size());
//        {
//            try {
//                PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=2");
//                ResultSet rs = ps.executeQuery();
//                rs.next();
//                rs.close();
//                Assert.fail();
//            } catch (Exception ex) {
//                // Assert.assertTrue(ex.getMessage().contains("transaction across group is not supported"));
//            }
//        }
//
//        Assert.assertEquals(context, conn.getExecutionContext());
//        Assert.assertEquals(t, conn.getExecutionContext().getTransaction());
//        Assert.assertEquals(1, ch.getAllConnection().size());
//        conn.commit();
//        conn.close();
//
//        Assert.assertEquals(0, ch.getAllConnection().size());
//
//    }
//
//    @Test
//    public void testAutoCommit() throws Exception {
//        TConnection conn = (TConnection) ds.getConnection();
//        StrictConnectionHolder ch = conn.getConnectionHolder();
//
//        conn.setAutoCommit(true);
//
//        ExecutionContext context = conn.getExecutionContext();
//        Assert.assertTrue(context.getTransaction() == null);
//        StrictlTransaction t1 = null;
//        {
//            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
//            ResultSet rs = ps.executeQuery();
//            rs.next();
//
//            context = conn.getExecutionContext();
//            Assert.assertTrue(context.getTransaction() != null);
//            t1 = (StrictlTransaction) context.getTransaction();
//            Assert.assertEquals("My_Transaction", t1.getClass().getSimpleName());
//            Assert.assertEquals("andor_show_group1", context.getTransactionGroup());
//
//            Assert.assertEquals(1, ch.getAllConnection().size());
//            rs.close();
//            Assert.assertEquals(1, ch.getAllConnection().size());
//        }
//
//        StrictlTransaction t2 = null;
//        {
//            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
//            ResultSet rs = ps.executeQuery();
//            context = conn.getExecutionContext();
//            rs.next();
//
//            t2 = (StrictlTransaction) context.getTransaction();
//            Assert.assertEquals(2, ch.getAllConnection().size());
//            rs.close();
//            Assert.assertEquals(1, ch.getAllConnection().size());
//        }
//
//        Assert.assertTrue(t1 != t2);
//        conn.commit();
//        conn.close();
//
//        Assert.assertEquals(0, ch.getAllConnection().size());
//    }
//
//    /**
//     * 先做非auto的，再做auto的 两次的连接都应该被关闭
//     * 
//     * @throws Exception
//     */
//    @Test
//    public void testNotAutoCommit2() throws Exception {
//        TConnection conn = (TConnection) ds.getConnection();
//
//        StrictConnectionHolder ch = conn.getConnectionHolder();
//
//        conn.setAutoCommit(false);
//        ExecutionContext context1 = conn.getExecutionContext();
//        Assert.assertTrue(context1.getTransaction() == null);
//        {
//            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
//            ResultSet rs = ps.executeQuery();
//
//            Assert.assertEquals(context1, conn.getExecutionContext());
//            rs.next();
//            rs.close();
//        }
//
//        Assert.assertTrue(context1.getTransaction() != null);
//        StrictlTransaction t1 = (StrictlTransaction) context1.getTransaction();
//
//        Assert.assertEquals("My_Transaction", t1.getClass().getSimpleName());
//        Assert.assertEquals("andor_show_group1", context1.getTransactionGroup());
//        Assert.assertEquals(1, ch.getAllConnection().size());
//        {
//            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=1");
//            ResultSet rs = ps.executeQuery();
//
//            Assert.assertEquals(context1, conn.getExecutionContext());
//            rs.next();
//            rs.close();
//        }
//
//        Assert.assertEquals(t1, conn.getExecutionContext().getTransaction());
//        Assert.assertEquals(1, ch.getAllConnection().size());
//        conn.commit();
//        // commit之后，连接关闭
//        Assert.assertEquals(0, ch.getAllConnection().size());
//        conn.setAutoCommit(true);
//
//        Assert.assertEquals(0, ch.getAllConnection().size());
//
//        {
//            PreparedStatement ps = conn.prepareStatement("select * from bmw_users where id=2");
//            ResultSet rs = ps.executeQuery();
//            rs.next();
//            rs.close();
//        }
//
//        ExecutionContext context2 = conn.getExecutionContext();
//        StrictlTransaction t2 = (StrictlTransaction) context2.getTransaction();
//        Assert.assertEquals("andor_show_group0", context2.getTransactionGroup());
//        Assert.assertTrue(context2 != context1);
//        Assert.assertEquals(t2, conn.getExecutionContext().getTransaction());
//        conn.close();
//        Assert.assertEquals(0, ch.getAllConnection().size());
//        Assert.assertEquals(0, ch.getAllConnection().size());
//    }
//
// }
