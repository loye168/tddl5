package com.taobao.tddl.qatest.matrix.transaction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.tddl.common.jdbc.ITransactionPolicy;
import com.taobao.tddl.matrix.jdbc.TConnection;
import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;

/**
 * <pre>
 * policy_1 : 完全禁止跨库事务
 *  policy_2 : autocommit=true时跨库写在多个库上commit  先不測
 *  policy_3 : 禁止跨库写，允许跨库读
 *  policy_4 : policy_2的基础上允许跨库读 			先不測
 *  policy_5 : 完全允许跨库事务
 * </pre>
 */
public class TransactionMultiGroupTest extends BaseMatrixTestCase {

    public TransactionMultiGroupTest(){
        BaseTestCase.normaltblTableName = "mysql_normaltbl_mutilGroup";
    }

    @Before
    public void initData() throws Exception {
        tddlUpdateData("DELETE FROM " + normaltblTableName, null);
        mysqlUpdateData("DELETE FROM " + normaltblTableName, null);
        setTxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
        setTxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB);
    }

    // policy_1 : 完全禁止跨库事务
    @Test
    public void testPolicy1Query() throws Exception {
        normaltblPrepare(0, MAX_DATA_SIZE);
        setTxPolicy(ITransactionPolicy.TDDL);

        String sql = "select * from " + normaltblTableName;
        tddlConnection.setAutoCommit(false);
        try {
            ResultSet rs = tddlQueryData(sql, null);
            while (rs.next()) {
            }
            Assert.fail();
        } catch (Exception e) {
            // e.printStackTrace();
        }
        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);

    }

    // policy_1 : 完全禁止跨库事务, 单个insert应该没问题
    @Test
    public void testPolicy1SingleInsert() throws Exception {
        setTxPolicy(ITransactionPolicy.TDDL);
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add(null);
        param.add(fl);

        try {
            execute(sql, param);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "NAME", "ID", "gmt_create", "GMT_TIMESTAMP", "GMT_DATETIME", "floatCol" };
        selectContentSameAssert(sql, columnParam, null);
        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        selectContentSameAssert(sql, columnParam, null);

    }

    // policy_1 : 完全禁止跨库事务, 涉及到多库的多次insert应该报错
    @Test
    public void testPolicy1MultiInsertCommit() throws Exception {
        setTxPolicy(ITransactionPolicy.TDDL);
        tddlConnection.setAutoCommit(false);

        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add(null);
        param.add(fl);

        List<Object> param1 = new ArrayList<Object>();
        param1.add(RANDOM_ID + 2);
        param1.add(RANDOM_INT);
        param1.add(gmtDay);
        param1.add(gmt);
        param1.add(gmt);
        param1.add(null);
        param1.add(fl);

        try {
            tddlUpdateData(sql, param);
            tddlUpdateData(sql, param1);
            Assert.fail();
        } catch (Exception e) {
            // e.printStackTrace();
        }
        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);

        // 第一条数据应该插入成功
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        ResultSet rs = tddlQueryData(sql, null);
        Assert.assertTrue(rs.next());
        Assert.assertFalse(rs.next());

        // 第二条数据应该失败
        sql = "select * from " + normaltblTableName + " where pk=" + (RANDOM_ID + 2);
        rs = tddlQueryData(sql, null);
        Assert.assertFalse(rs.next());
    }

    // policy_1 : 完全禁止跨库事务, 涉及到多库的多次insert应该报错
    @Test
    public void testPolicy1MultiInsertRollback() throws Exception {
        setTxPolicy(ITransactionPolicy.TDDL);
        tddlConnection.setAutoCommit(false);

        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add(null);
        param.add(fl);

        List<Object> param1 = new ArrayList<Object>();
        param1.add(RANDOM_ID + 2);
        param1.add(RANDOM_INT);
        param1.add(gmtDay);
        param1.add(gmt);
        param1.add(gmt);
        param1.add(null);
        param1.add(fl);

        try {
            tddlUpdateData(sql, param);
            tddlUpdateData(sql, param1);
            Assert.fail();
        } catch (Exception e) {

        }
        tddlConnection.rollback();
        tddlConnection.setAutoCommit(true);

        // 第一条数据应该插入失败
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        ResultSet rs = tddlQueryData(sql, null);
        Assert.assertFalse(rs.next());

        // 第二条数据应该失败
        sql = "select * from " + normaltblTableName + " where pk=" + (RANDOM_ID + 2);
        rs = tddlQueryData(sql, null);
        Assert.assertFalse(rs.next());
    }

    // policy_1 : 完全禁止跨库事务, update涉及多库, autocommit不是跨庫事務，不會報錯
    @Test
    public void testPolicy1UpdateWithAutoCommit() throws Exception {
        normaltblPrepare(0, MAX_DATA_SIZE);
        setTxPolicy(ITransactionPolicy.TDDL);
        String sql = "update " + normaltblTableName + " set id=?, gmt_create=?,floatCol=?";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(fl);
        try {
            execute(sql, param);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "NAME", "ID", "gmt_create", "GMT_TIMESTAMP", "GMT_DATETIME", "floatCol" };
        selectContentSameAssert(sql, columnParam, null);

    }

    // policy_1 : 完全禁止跨库事务, update涉及多库时，应该报错
    @Test
    public void testPolicy1UpdateWithNotAutoCommit() throws Exception {
        setTxPolicy(ITransactionPolicy.TDDL);
        tddlConnection.setAutoCommit(false);
        String sql = "update " + normaltblTableName + " set id=?, gmt_create=?,floatCol=?";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(fl);
        try {
            tddlUpdateData(sql, param);
            Assert.fail();
        } catch (Exception e) {

        }
        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);

    }

    // policy_3 : 禁止跨库写，允许跨库读
    @Test
    public void testPolicy3Query() throws Exception {
        normaltblPrepare(0, MAX_DATA_SIZE);
        setTxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB);

        String sql = "select * from " + normaltblTableName;
        String[] columnParam = { "PK", "NAME", "ID", "gmt_create", "GMT_TIMESTAMP", "GMT_DATETIME", "floatCol" };
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        try {
            selectContentSameAssert(sql, columnParam, null);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

    }

    // policy_3 : 禁止跨库写，允许跨库读
    @Test
    public void testPolicy3Insert() throws Exception {
        setTxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB);
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add(null);
        param.add(fl);

        try {
            execute(sql, param);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "NAME", "ID", "gmt_create", "GMT_TIMESTAMP", "GMT_DATETIME", "floatCol" };
        selectContentSameAssert(sql, columnParam, null);
        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        selectContentSameAssert(sql, columnParam, null);

    }

    // policy_3 : 禁止跨库写，允许跨库读
    @Test
    public void testPolicy3Update() throws Exception {
        setTxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB);
        String sql = "update " + normaltblTableName + " set id=?, gmt_create=?,floatCol=?";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(fl);
        tddlConnection.setAutoCommit(false);
        try {
            tddlUpdateData(sql, param);
            Assert.fail();
        } catch (Exception e) {
        }

        tddlConnection.commit();
        tddlConnection.setAutoCommit(true);

    }

    // policy_5 : 完全允许跨库事务
    @Test
    public void testPolicy5Query() throws Exception {
        normaltblPrepare(0, MAX_DATA_SIZE);
        setTxPolicy(ITransactionPolicy.FREE);

        String sql = "select * from " + normaltblTableName;
        String[] columnParam = { "PK", "NAME", "ID", "gmt_create", "GMT_TIMESTAMP", "GMT_DATETIME", "floatCol" };
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        try {
            selectContentSameAssert(sql, columnParam, null);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

    }

    // policy_5 : 完全允许跨库事务
    @Test
    public void testPolicy5Insert() throws Exception {
        setTxPolicy(ITransactionPolicy.FREE);
        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(gmt);
        param.add(gmt);
        param.add(null);
        param.add(fl);

        try {
            execute(sql, param);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "NAME", "ID", "gmt_create", "GMT_TIMESTAMP", "GMT_DATETIME", "floatCol" };
        selectContentSameAssert(sql, columnParam, null);
        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        selectContentSameAssert(sql, columnParam, null);

    }

    // policy_5 : 完全允许跨库事务
    @Test
    public void testPolicy5UpdateWithAutoCommit() throws Exception {
        setTxPolicy(ITransactionPolicy.FREE);
        String sql = "update " + normaltblTableName + " set id=?, gmt_create=?,floatCol=?";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(fl);

        try {
            execute(sql, param);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "NAME", "ID", "gmt_create", "GMT_TIMESTAMP", "GMT_DATETIME", "floatCol" };
        selectContentSameAssert(sql, columnParam, null);

    }

    // policy_5 : 完全允许跨库事务
    @Test
    public void testPolicy5UpdateWithNotAutoCommit() throws Exception {
        setTxPolicy(ITransactionPolicy.FREE);
        String sql = "update " + normaltblTableName + " set id=?, gmt_create=?,floatCol=?";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_INT);
        param.add(gmtDay);
        param.add(fl);

        tddlConnection.setAutoCommit(false);
        mysqlConnection.setAutoCommit(false);
        try {
            execute(sql, param);

            sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
            String[] columnParam = { "PK", "NAME", "ID", "gmt_create", "GMT_TIMESTAMP", "GMT_DATETIME", "floatCol" };
            selectContentSameAssert(sql, columnParam, null);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        tddlConnection.commit();
        mysqlConnection.commit();
        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "NAME", "ID", "gmt_create", "GMT_TIMESTAMP", "GMT_DATETIME", "floatCol" };
        selectContentSameAssert(sql, columnParam, null);
    }

    private void setTxPolicy(ITransactionPolicy trxPolicy) throws SQLException {
        if (tddlConnection instanceof TConnection) {
            ((TConnection) tddlConnection).setTrxPolicy(trxPolicy);
        } else {
            Statement stmt = tddlConnection.createStatement();
            int code = 1;
            if (trxPolicy == ITransactionPolicy.TDDL) {
                code = 1;
            } else if (trxPolicy == ITransactionPolicy.ALLOW_READ_CROSS_DB) {
                code = 3;
            } else if (trxPolicy == ITransactionPolicy.FREE) {
                code = 5;
            }
            stmt.execute("set transaction policy " + code);
        }
    }
}
