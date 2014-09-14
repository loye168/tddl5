package com.taobao.tddl.qatest.matrix.set;

import java.sql.ResultSet;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.matrix.jdbc.TConnection;
import com.taobao.tddl.qatest.BaseMatrixTestCase;

public class SetCommandTest extends BaseMatrixTestCase {

    @Test
    public void testSetNamesGbk() throws Exception {
        setName("gbk");

        String sql = "show variables like 'character_set_client'";
        ResultSet rs = tddlQueryData(sql, null);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("Value").equals("gbk"));
        Assert.assertFalse(rs.next());

        sql = "show variables like 'character_set_connection'";
        rs = tddlQueryData(sql, null);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("Value").equals("gbk"));
        Assert.assertFalse(rs.next());

        sql = "show variables like 'character_set_results'";
        rs = tddlQueryData(sql, null);
        Assert.assertTrue(rs.next());
        // Assert.assertTrue(rs.getString("Value").equals("gbk"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSetNamesUtf8() throws Exception {
        setName("utf8");

        String sql = "show variables like 'character_set_client'";
        ResultSet rs = tddlQueryData(sql, null);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("Value").equals("utf8"));
        Assert.assertFalse(rs.next());

        sql = "show variables like 'character_set_connection'";
        rs = tddlQueryData(sql, null);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("Value").equals("utf8"));
        Assert.assertFalse(rs.next());

        sql = "show variables like 'character_set_results'";
        rs = tddlQueryData(sql, null);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString("Value").equals("utf8"));
        Assert.assertFalse(rs.next());
    }

    private void setName(String encode) throws Exception {
        if (isTddlServer()) {
            String sql = "set names " + encode;
            tddlUpdateData(sql, null);
        } else {
            ((TConnection) tddlConnection).setEncoding(encode);
        }
    }

}
