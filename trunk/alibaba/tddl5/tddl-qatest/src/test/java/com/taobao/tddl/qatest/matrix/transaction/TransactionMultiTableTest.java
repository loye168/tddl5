package com.taobao.tddl.qatest.matrix.transaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * 多表transaction
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
@RunWith(EclipseParameterized.class)
public class TransactionMultiTableTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblStudentTable(dbType));
    }

    public TransactionMultiTableTest(String normaltblTableName, String studentTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
        BaseTestCase.studentTableName = studentTableName;
    }

    @Before
    public void initData() throws Exception {
        tddlUpdateData("DELETE FROM " + studentTableName, null);
        mysqlUpdateData("DELETE FROM " + studentTableName, null);
        tddlUpdateData("DELETE FROM " + normaltblTableName, null);
        mysqlUpdateData("DELETE FROM " + normaltblTableName, null);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);

        tddlConnection.setAutoCommit(true);
        mysqlConnection.setAutoCommit(true);
    }

    /**
     * commit
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void testCommit() throws Exception {

        if (!normaltblTableName.substring(0, 2).equals(studentTableName.substring(0, 2))) {
            // 跨库事务暂不支持
            return;
        }
        String sql = "insert into " + normaltblTableName + "(pk,id) values(" + RANDOM_ID + "," + RANDOM_INT + ")";
        tddlConnection.setAutoCommit(false);

        mysqlConnection.setAutoCommit(false);
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, null);
            int andorAffectRow = tddlUpdateDataTranscation(sql, null);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            sql = "insert into " + studentTableName + " (id,name,school) values (?,?,?)";
            List<Object> param = new ArrayList<Object>();
            param.add(RANDOM_ID);
            param.add(name);
            param.add(school);

            mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            andorAffectRow = tddlUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            mysqlConnection.commit();
            tddlConnection.commit();
        } catch (Exception e) {
            try {
                mysqlConnection.rollback();
                tddlConnection.rollback();
            } catch (Exception ee) {

            }
        }
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam1 = { "ID" };
        selectOrderAssertTranscation(sql, columnParam1, null);
        sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;
        String[] columnParam = { "NAME", "SCHOOL" };
        selectOrderAssertTranscation(sql, columnParam, null);
    }

    /**
     * rollback
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void testRollback() throws Exception {
        if (!normaltblTableName.substring(0, 2).equals(studentTableName.substring(0, 2))) {
            // 跨库事务暂不支持
            return;
        }
        String sql = "insert into " + normaltblTableName + "(pk,id) values(" + RANDOM_ID + "," + RANDOM_INT + ")";
        tddlConnection.setAutoCommit(false);

        mysqlConnection.setAutoCommit(false);
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, null);
            int andorAffectRow = tddlUpdateDataTranscation(sql, null);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            sql = "insert into " + studentTableName + " (id,name,school) values (?,?,?)";
            List<Object> param = new ArrayList<Object>();
            param.add(RANDOM_ID);
            param.add(name);
            param.add(school);

            mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            andorAffectRow = tddlUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            mysqlConnection.rollback();
            tddlConnection.rollback();
        } catch (Exception e) {
            try {
                mysqlConnection.rollback();
                tddlConnection.rollback();
            } catch (Exception ee) {

            }
        }
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam1 = { "ID" };
        selectOrderAssertTranscation(sql, columnParam1, null);
        sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;
        String[] columnParam = { "NAME", "SCHOOL" };
        selectOrderAssertTranscation(sql, columnParam, null);

    }

    /**
     * rollback之前
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void testBeforeRollback() throws Exception {
        if (!normaltblTableName.substring(0, 2).equals(studentTableName.substring(0, 2))) {
            // 跨库事务暂不支持
            return;
        }

        // TODO:ob bug，读取不到事务内的最新数据
        if (normaltblTableName.startsWith("ob")) {
            return;
        }

        String sql = "insert into " + normaltblTableName + "(pk,id) values(" + RANDOM_ID + "," + RANDOM_INT + ")";
        tddlConnection.setAutoCommit(false);
        String[] columnParam1 = { "ID" };
        String[] columnParam = { "NAME", "SCHOOL" };
        mysqlConnection.setAutoCommit(false);
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, null);
            int andorAffectRow = tddlUpdateDataTranscation(sql, null);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            sql = "insert into " + studentTableName + " (id,name,school) values (?,?,?)";
            List<Object> param = new ArrayList<Object>();
            param.add(RANDOM_ID);
            param.add(name);
            param.add(school);

            mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            andorAffectRow = tddlUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);
            sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;

            selectOrderAssertTranscation(sql, columnParam1, null);
            sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;

            selectOrderAssertTranscation(sql, columnParam, null);
            mysqlConnection.rollback();
            tddlConnection.rollback();
        } catch (Exception e) {
            try {
                mysqlConnection.rollback();
                tddlConnection.rollback();
            } catch (Exception ee) {

            }
        }
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        selectOrderAssertTranscation(sql, columnParam1, null);
        sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;
        selectOrderAssertTranscation(sql, columnParam, null);
    }
}
