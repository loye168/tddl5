package com.taobao.tddl.qatest.matrix.set;

import java.sql.ResultSet;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.tddl.matrix.jdbc.TConnection;
import com.taobao.tddl.qatest.BaseMatrixTestCase;

public class SetSqlModeTest extends BaseMatrixTestCase {

    // HIGH_NOT_PRECEDENCE
    @Test
    public void testSet_HIGH_NOT_PRECEDENCE() throws Exception {
        setSqlMode("HIGH_NOT_PRECEDENCE");
        String sql = "SELECT NOT 1 BETWEEN -5 AND 5 as col";
        ResultSet rs = tddlQueryData(sql, null);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("col").equals("1"));
        Assert.assertFalse(rs.next());

        setSqlMode(" ");
        sql = "SELECT NOT 1 BETWEEN -5 AND 5 as col";
        rs = tddlQueryData(sql, null);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("col").equals("0"));
        Assert.assertFalse(rs.next());
    }

    // IGNORE_SPACE
    @Test
    public void testSet_IGNORE_SPACE() throws Exception {
        setSqlMode("IGNORE_SPACE");
        String sql = "select count (*) from MYSQL_NORMALTBL_ONEGROUP_ONEATOM";
        try {
            ResultSet rs = tddlQueryData(sql, null);
            while (rs.next()) {

            }
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        setSqlMode(" ");
        sql = "select count (*) from MYSQL_NORMALTBL_ONEGROUP_ONEATOM";
        try {
            ResultSet rs = tddlQueryData(sql, null);
            while (rs.next()) {

            }
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("execute error by You have an error in your SQL syntax"));
        }
    }

    // STRICT_TRANS_TABLES
    @Test
    public void testSet_STRICT_TRANS_TABLES() throws Exception {
        execute("delete from mysql_normaltbl_mutilgroup", null);

        // name字段值超长，strict模式应该报错
        setSqlMode("STRICT_TRANS_TABLES");
        String sql = "insert into mysql_normaltbl_mutilgroup(pk,name) values(1,'abcdeabcdeabcdeabcdeabcde')";
        try {
            tddlUpdateData(sql, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Data too long for column 'name'"));
        }

        setSqlMode(" ");
        sql = "insert into mysql_normaltbl_mutilgroup(pk,name) values(1,'abcdeabcdeabcdeabcdeabcde')";
        try {
            tddlUpdateData(sql, null);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = "select * from mysql_normaltbl_mutilgroup";
        ResultSet rs = tddlQueryData(sql, null);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("name").equals("abcdeabcdeabcdeabcde"));
        Assert.assertFalse(rs.next());
    }

    // ANSI
    @Ignore("暂不支持")
    @Test
    public void testSet_ANSI() throws Exception {
        setSqlMode("ANSI");
        String sql = "SELECT * FROM mysql_normaltbl_mutilgroup t1 WHERE t1.id IN "
                     + "(SELECT MAX(t1.id) FROM mysql_normaltbl_onegroup_oneatom )";
        try {
            tddlQueryData(sql, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("execute error by You have an error in your SQL syntax"));
        }

        setSqlMode(" ");
        sql = "SELECT * FROM mysql_normaltbl_mutilgroup t1 WHERE t1.id IN "
              + "(SELECT MAX(t1.id) FROM mysql_normaltbl_onegroup_oneatom )";
        try {
            String[] columnParam = new String[] { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP",
                    "GMT_DATETIME" };
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    private void setSqlMode(String mode) throws Exception {
        if (isTddlServer()) {
            String sql = "SET session sql_mode = '" + mode + "'";
            tddlUpdateData(sql, null);
        } else {
            ((TConnection) tddlConnection).setSqlMode(mode);
        }
    }
}
