package com.taobao.tddl.optimizer.parse;

import java.sql.SQLSyntaxErrorException;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.recognizer.SQLParserDelegate;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MysqlTableVisitor;

/**
 * 测试下表名提取
 * 
 * @author jianghang 2014-3-12 上午10:39:35
 * @since 5.0.0
 */
public class TableParserTest extends BaseOptimizerTest {

    @Test
    public void testInert() throws SQLSyntaxErrorException {
        String sql = "insert into sc.table1 values(?,?,?,?)";
        Assert.assertEquals("TABLE1 ", buildTableNames(sql));

        sql = "insert into table1 select * from table2";
        Assert.assertEquals("TABLE1 TABLE2 ", buildTableNames(sql));

        sql = "insert into table1 values(?,?,?,?) on duplicate key update col1 = ?";
        Assert.assertEquals("TABLE1 ", buildTableNames(sql));

        sql = "replace into sc.table1 values(?,?,?,?)";
        Assert.assertEquals("TABLE1 ", buildTableNames(sql));
    }

    @Test
    public void testUpdate() throws SQLSyntaxErrorException {
        String sql = "update table2 set col1=?,col2=? where col3=?";
        Assert.assertEquals("TABLE2 ", buildTableNames(sql));

        sql = "update table2 set col1=?,col2=? where id=(select id from table3)";
        Assert.assertEquals("TABLE3 TABLE2 ", buildTableNames(sql));

        sql = "update table2 set col1=?,col2=? where id=(select id from (select id from table4 limit 1) as a join table3 on table3.id = a.id)";
        Assert.assertEquals("TABLE3 TABLE2 TABLE4 ", buildTableNames(sql));
    }

    @Test
    public void testDelete() throws SQLSyntaxErrorException {
        String sql = "delete from sc.table3.* where id=(select id from table4)";
        Assert.assertEquals("TABLE3 TABLE4 ", buildTableNames(sql));
    }

    @Test
    public void testSelect() throws SQLSyntaxErrorException {
        String sql = "select * from table2,table3";
        Assert.assertEquals("TABLE3 TABLE2 ", buildTableNames(sql));

        sql = "select * from table2,(select * from table3) as t";
        Assert.assertEquals("TABLE3 TABLE2 ", buildTableNames(sql));

        sql = "select * from table2 t,table3 where t.id=(select id from table3)";
        Assert.assertEquals("TABLE3 TABLE2 ", buildTableNames(sql));
    }

    @Test
    public void testShow() throws SQLSyntaxErrorException {
        String sql = "show create table table2";
        Assert.assertEquals("TABLE2 ", buildTableNames(sql));

        sql = "show columns from table2";
        Assert.assertEquals("TABLE2 ", buildTableNames(sql));
    }

    private String buildTableNames(String sql) throws SQLSyntaxErrorException {
        SQLStatement statement = SQLParserDelegate.parse(sql);
        MysqlTableVisitor visitor = new MysqlTableVisitor();
        statement.accept(visitor);

        StringBuilder result = new StringBuilder();
        for (String table : visitor.getTablesWithSchema().keySet()) {
            result.append(table + " ");
        }
        return result.toString();
    }

}
