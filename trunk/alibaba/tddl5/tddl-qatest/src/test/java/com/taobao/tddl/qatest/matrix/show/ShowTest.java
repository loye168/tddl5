package com.taobao.tddl.qatest.matrix.show;

import java.sql.ResultSet;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.qatest.BaseMatrixTestCase;

public class ShowTest extends BaseMatrixTestCase {

    @Test
    public void testShowSequence() {
        String sql = "delete from sequence where name like 'chenhui'";
        try {
            tddlUpdateData(sql, null);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = "create sequence chenhui start with 100";
        try {
            tddlUpdateData(sql, null);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        sql = "show sequences";

        boolean existName = false;
        boolean valueRight = false;
        try {
            ResultSet rs = tddlQueryData(sql, null);
            while (rs.next()) {
                if (rs.getString("name").equals("chenhui")) {
                    existName = true;
                }
                if (rs.getLong("value") == 100) {
                    valueRight = true;
                }
            }
            Assert.assertTrue(existName);
            Assert.assertTrue(valueRight);

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testShowTables() throws Exception {
        String sql = "show tables";

        ResultSet rs = tddlQueryData(sql, null);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    @Test
    public void testShowSpecitifedTable() {
        String sql = "show tables like 'MYSQL_NORMALTBL_ONEGROUP_ONEATOM'";

        try {
            ResultSet rs = tddlQueryData(sql, null);
            Assert.assertTrue(rs.next());

            Assert.assertEquals(rs.getString(1), "MYSQL_NORMALTBL_ONEGROUP_ONEATOM");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testShowTablesNotExist() {
        String sql = "show tables like 'abc'";

        try {
            ResultSet rs = tddlQueryData(sql, null);
            Assert.assertFalse(rs.next());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testShowBroadCasts() {

    }

    @Test
    public void testShowPartitions() {
        String sql = "show partitions from MYSQL_NORMALTBL_MUTILGROUP";
        try {
            ResultSet rs = tddlQueryData(sql, null);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString("KEYS"), "PK");
            Assert.assertFalse(rs.next());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testShowPartitionsFromNotExistTable() {
        String sql = "show partitions from not_exist_table";
        try {
            ResultSet rs = tddlQueryData(sql, null);
            while (rs.next()) {

            }
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not found table : NOT_EXIST_TABLE"));
        }
    }

    @Test
    public void testShowRule() {
        String sql = "show rule from MYSQL_NORMALTBL_MUTILGROUP";
        try {
            ResultSet rs = tddlQueryData(sql, null);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString("ID"), "0");
            Assert.assertEquals(rs.getString("TABLE_NAME"), "MYSQL_NORMALTBL_MUTILGROUP");
            Assert.assertEquals(rs.getString("BROADCAST"), "false");
            Assert.assertEquals(rs.getString("ALLOW_FULL_TABLE_SCAN"), "true");
            Assert.assertEquals(rs.getString("DB_NAME_PATTERN"), "andor_mysql_group_{0}");
            Assert.assertEquals(rs.getString("DB_RULES_STR"), "Math.abs(Long.valueOf(#pk,1,4#) %4).intdiv(2)");
            Assert.assertEquals(rs.getString("TB_NAME_PATTERN"), "mysql_normaltbl_mutilGroup_{00}");
            Assert.assertEquals(rs.getString("TB_RULES_STR"), "Math.abs(Long.valueOf(#pk,1,4#)) % 4 %2");
            Assert.assertEquals(rs.getString("PARTITION_KEYS"), "[PK]");
            Assert.assertEquals(rs.getString("DEFAULT_DB_INDEX"), "andor_mysql_group_oneAtom");
            Assert.assertFalse(rs.next());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testShowRuleWithNotExistTable() {
        String sql = "show rule from NOT_EXIST_TABLE";
        try {
            ResultSet rs = tddlQueryData(sql, null);
            while (rs.next()) {

            }
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not found table : NOT_EXIST_TABLE"));
        }
    }

    @Test
    public void testShowTopology() {
        String sql = "show topology from MYSQL_NORMALTBL_MUTILGROUP;";
        try {
            ResultSet rs = tddlQueryData(sql, null);
            int idx = 0;
            while (rs.next()) {
                if (idx == 0) {
                    Assert.assertEquals(rs.getString("GROUP_NAME"), "andor_mysql_group_1");
                    Assert.assertEquals(rs.getString("TABLE_NAME"), "mysql_normaltbl_mutilGroup_00");
                } else if (idx == 1) {
                    Assert.assertEquals(rs.getString("GROUP_NAME"), "andor_mysql_group_1");
                    Assert.assertEquals(rs.getString("TABLE_NAME"), "mysql_normaltbl_mutilGroup_01");
                } else if (idx == 2) {
                    Assert.assertEquals(rs.getString("GROUP_NAME"), "andor_mysql_group_0");
                    Assert.assertEquals(rs.getString("TABLE_NAME"), "mysql_normaltbl_mutilGroup_00");
                } else if (idx == 3) {
                    Assert.assertEquals(rs.getString("GROUP_NAME"), "andor_mysql_group_0");
                    Assert.assertEquals(rs.getString("TABLE_NAME"), "mysql_normaltbl_mutilGroup_01");
                    break;
                }
                idx++;
            }

            Assert.assertFalse(rs.next());

        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testShowTopologyWithNotExistTable() {
        String sql = "show topology from NOT_EXIST_TABLE;";
        try {
            ResultSet rs = tddlQueryData(sql, null);
            while (rs.next()) {

            }
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not found table : NOT_EXIST_TABLE"));
        }
    }
}
