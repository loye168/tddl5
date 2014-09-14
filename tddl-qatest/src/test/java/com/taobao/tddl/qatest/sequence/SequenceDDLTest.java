package com.taobao.tddl.qatest.sequence;

import java.sql.ResultSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.tddl.qatest.BaseMatrixTestCase;

/**
 * sequence ddl
 * 
 * @author chenhui
 * @since 5.1.0
 */

public class SequenceDDLTest extends BaseMatrixTestCase {

    @Before
    public void initData() throws Exception {
        tddlUpdateData("delete from sequence where name like 'chenhui'", null);
        // mysqlUpdateData("DELETE FROM " + studentTableName, null);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    @Test
    public void testCreateSequence() {
        String sql = "create sequence chenhui start with 100";
        try {
            tddlUpdateData(sql, null);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        assertExistsSequence("chenhui", 100);
    }

    @Test
    public void testDeleteSequence() {
        String sql = "create sequence chenhui start with 1";
        try {
            tddlUpdateData(sql, null);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        assertExistsSequence("chenhui", 1);

        sql = "delete from sequence where name like 'chenhui'";
        try {
            tddlUpdateData(sql, null);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        assertNotExistsSequence("chenhui");
    }

    @Test
    public void testSequenceValue() {
        String sql = "create sequence chenhui start with 10";
        try {
            tddlUpdateData(sql, null);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        assertExistsSequence("chenhui", 10);

        try {
            sql = "select chenhui.nextval  from dual";
            ResultSet rs = tddlQueryData(sql, null);
            Assert.assertTrue(rs.next());
            long seqVal = rs.getLong("chenhui.nextVal");

            sql = "select chenhui.nextVal  from dual";
            rs = tddlQueryData(sql, null);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getLong("chenhui.nextVal"), seqVal + 1);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void testSequenceValueWithCoronaMethod() {
        String sql = "create sequence chenhui start with 10";
        try {
            tddlUpdateData(sql, null);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        assertExistsSequence("chenhui", 10);

        try {
            sql = "select corona_next_val  from chenhui";
            ResultSet rs = tddlQueryData(sql, null);
            Assert.assertTrue(rs.next());
            long seqVal = rs.getLong("DRDS_SEQ_VAL");

            sql = "select corona_next_val  from chenhui";
            rs = tddlQueryData(sql, null);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getLong("DRDS_SEQ_VAL"), seqVal + 1);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void assertExistsSequence(String name, int num) {
        String sql = "show sequences";

        boolean existName = false;
        boolean valueRight = false;
        try {
            ResultSet rs = tddlQueryData(sql, null);
            while (rs.next()) {
                if (rs.getString("name").equals(name)) {
                    existName = true;
                }
                if (rs.getLong("value") == num) {
                    valueRight = true;
                }
            }
            Assert.assertTrue(existName);
            Assert.assertTrue(valueRight);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void assertNotExistsSequence(String name) {
        String sql = "show sequences";
        boolean existName = false;
        try {
            ResultSet rs = tddlQueryData(sql, null);
            while (rs.next()) {
                if (rs.getString("name").equals(name)) {
                    existName = true;
                }
            }
            Assert.assertFalse(existName);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
