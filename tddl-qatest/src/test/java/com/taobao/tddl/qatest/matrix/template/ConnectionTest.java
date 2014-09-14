package com.taobao.tddl.qatest.matrix.template;

import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
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
 * 各种tddlConnectionnection的方法测试
 * 
 * @author jianghang 2014-3-19 下午4:51:37
 * @since 5.0.2
 */
@RunWith(EclipseParameterized.class)
public class ConnectionTest extends BaseMatrixTestCase {

    public ConnectionTest(String studentTableName){
        BaseTestCase.studentTableName = studentTableName;
    }

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.studentTable(dbType));
    }

    @Before
    public void initData() throws Exception {
        tddlUpdateData("delete from  " + studentTableName, null);
        mysqlUpdateData("delete from  " + studentTableName, null);
    }

    /**
     * prepareStatement
     * 
     * @author jianghang
     * @since 5.0.2
     */
    @Test
    public void testprepareStatement() throws Exception {
        PreparedStatement ps = tddlConnection.prepareStatement("insert into " + studentTableName
                                                               + " (id,name,school) values (?,?,?)");
        ps.setObject(1, RANDOM_ID);
        ps.setObject(2, name);
        ps.setObject(3, school);
        int affect = ps.executeUpdate();
        ps.close();
        Assert.assertTrue(affect == 1);

        String sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;
        ps = tddlConnection.prepareStatement(sql);
        ResultSet result = ps.executeQuery();
        Assert.assertTrue(result.next());
        ps.close();

        sql = "delete from " + studentTableName + " where id= ? ";
        ps = tddlConnection.prepareStatement(sql);
        ps.setObject(1, RANDOM_ID);
        boolean success = ps.execute();
        ps.close();
        Assert.assertTrue(success == false);
    }

    /**
     * prepareStatement 各种参数
     * 
     * @author jianghang
     * @since 5.0.2
     */
    @Test
    public void testprepareStatement_各种参数() throws Exception {
        PreparedStatement ps = tddlConnection.prepareStatement("insert into " + studentTableName
                                                               + " (id,name,school) values (?,?,?)",
            Statement.RETURN_GENERATED_KEYS);
        ps.setObject(1, RANDOM_ID);
        ps.setObject(2, name);
        ps.setObject(3, school);
        int affect = ps.executeUpdate();
        ps.close();
        Assert.assertTrue(affect == 1);

        String sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;
        ps = tddlConnection.prepareStatement(sql,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT);
        ResultSet result = ps.executeQuery();
        Assert.assertTrue(result.next());
        ps.close();

        sql = "delete from " + studentTableName + " where id = ?";
        ps = tddlConnection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setObject(1, RANDOM_ID);
        boolean success = ps.execute();
        ps.close();
        Assert.assertTrue(success == false);

        ps = tddlConnection.prepareStatement("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            new String[] { "id", "name", "school" });
        ps.setObject(1, RANDOM_ID);
        ps.setObject(2, name);
        ps.setObject(3, school);
        affect = ps.executeUpdate();
        ps.close();
        Assert.assertTrue(affect == 1);

        sql = "delete from " + studentTableName + " where id= ? ";
        ps = tddlConnection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setObject(1, RANDOM_ID);
        success = ps.execute();
        ps.close();
        Assert.assertTrue(success == false);

        ps = tddlConnection.prepareStatement("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            new int[] { 0, 1, 2 });
        ps.setObject(1, RANDOM_ID);
        ps.setObject(2, name);
        ps.setObject(3, school);
        affect = ps.executeUpdate();
        ps.close();
        Assert.assertTrue(affect == 1);

        sql = "delete from " + studentTableName + " where id= ? ";
        ps = tddlConnection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ps.setObject(1, RANDOM_ID);
        success = ps.execute();
        ps.close();
        Assert.assertTrue(success == false);
    }

    /**
     * Statement
     * 
     * @author jianghang
     * @since 5.0.2
     */
    @Test
    public void testStatement() throws Exception {
        PreparedStatement ps = tddlConnection.prepareStatement("insert into " + studentTableName
                                                               + " (id,name,school) values (?,?,?)");
        ps.setObject(1, RANDOM_ID);
        ps.setObject(2, name);
        ps.setObject(3, school);
        int affect = ps.executeUpdate();
        ps.close();
        Assert.assertTrue(affect == 1);

        Statement stmt = tddlConnection.createStatement();
        String sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;
        ResultSet result = stmt.executeQuery(sql);
        Assert.assertTrue(result.next());
        stmt.close();

        sql = "delete from " + studentTableName + " where id=" + RANDOM_ID;
        stmt = tddlConnection.createStatement();
        boolean success = stmt.execute(sql);
        stmt.close();
        Assert.assertTrue(success == false);
    }

    /**
     * Statement 各种参数
     * 
     * @author jianghang
     * @since 5.0.2
     */
    @Test
    public void testStatement_各种参数() throws Exception {
        PreparedStatement ps = tddlConnection.prepareStatement("insert into " + studentTableName
                                                               + " (id,name,school) values (?,?,?)");
        ps.setObject(1, RANDOM_ID);
        ps.setObject(2, name);
        ps.setObject(3, school);
        int affect = ps.executeUpdate();
        ps.close();
        Assert.assertTrue(affect == 1);

        Statement stmt = tddlConnection.createStatement();
        String sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;
        ResultSet result = stmt.executeQuery(sql);
        Assert.assertTrue(result.next());
        stmt.close();

        stmt = tddlConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        result = stmt.executeQuery(sql);
        Assert.assertTrue(result.next());
        stmt.close();

        stmt = tddlConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT);
        result = stmt.executeQuery(sql);
        Assert.assertTrue(result.next());
        stmt.close();

        sql = "delete from " + studentTableName + " where id=" + RANDOM_ID;
        stmt = tddlConnection.createStatement();
        boolean success = stmt.execute(sql);
        stmt.close();
        Assert.assertTrue(success == false);
    }

    /**
     * call 存储过程
     * 
     * @author jianghang
     * @since 5.0.2
     */
    @Test
    public void testCallStatement() throws Exception {
        CallableStatement stmt = tddlConnection.prepareCall("{call proc_in_test(?)}");
        stmt.setInt(1, 0);
        ResultSet cursor = stmt.executeQuery();
        Assert.assertTrue(cursor.next());
        System.out.println(cursor.getString(1));
        stmt.close();

        if (isTddlServer()) {
            // server模式不支持call outparameter
            return;
        }

        stmt = tddlConnection.prepareCall("{call proc_out_test(?,?)}",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY);
        stmt.setInt(1, 0);
        stmt.registerOutParameter(2, Types.VARCHAR); // 注册出参
        boolean result = stmt.execute();
        Assert.assertTrue(result == false);
        System.out.println(stmt.getString(2));
        stmt.close();

        stmt = tddlConnection.prepareCall(" { call proc_out_test(?,?)}",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT);
        stmt.setInt(1, 0);
        stmt.registerOutParameter(2, Types.VARCHAR); // 注册出参
        result = stmt.execute();
        Assert.assertTrue(result == false);
        System.out.println(stmt.getString(2));
        stmt.close();
    }

    /**
     * show
     * 
     * @author jianghang
     * @since 5.0.2
     */
    @Test
    public void testShow() throws Exception {
        PreparedStatement ps = tddlConnection.prepareStatement("show tables");
        ResultSet rs = ps.executeQuery();
        printResult(rs);
        rs.close();
        ps.close();

        ps = tddlConnection.prepareStatement("show create table mysql_ibatis_type");
        rs = ps.executeQuery();
        printResult(rs);
        rs.close();
        ps.close();

        ps = tddlConnection.prepareStatement("show index from mysql_ibatis_type");
        rs = ps.executeQuery();
        printResult(rs);
        rs.close();
        ps.close();

        ps = tddlConnection.prepareStatement("show columns from mysql_ibatis_type");
        rs = ps.executeQuery();
        printResult(rs);
        rs.close();
        ps.close();

        ps = tddlConnection.prepareStatement("show full columns from mysql_ibatis_type");
        rs = ps.executeQuery();
        printResult(rs);
        rs.close();
        ps.close();

        ps = tddlConnection.prepareStatement("desc mysql_ibatis_type");
        rs = ps.executeQuery();
        printResult(rs);
        rs.close();
        ps.close();

        ps = tddlConnection.prepareStatement("select last_insert_id()");
        rs = ps.executeQuery();
        printResult(rs);
        rs.close();
        ps.close();
    }

    /**
     * MetaData
     * 
     * @author jianghang
     * @since 5.0.2
     */
    @Test
    public void testDatabaseMetaData() throws Exception {
        DatabaseMetaData metaData = tddlConnection.getMetaData();
        ResultSet rs = metaData.getTableTypes();
        printResult(rs);

        rs = metaData.getTables("andor_mysql_group_atom",
            "andor_mysql_group_atom",
            "mysql_ibatis_type",
            new String[] { "TABLE" });
        printResult(rs);

        rs = metaData.getColumns("andor_mysql_group_atom", "andor_mysql_group_atom", "mysql_ibatis_type", "%");
        printResult(rs);
    }

    /**
     * printResult
     * 
     * @author jianghang
     * @since 5.0.2
     */
    private void printResult(ResultSet rs) throws SQLException {
        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            int count = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= count; i++) {
                String key = rs.getMetaData().getColumnLabel(i);
                Object val = rs.getObject(i);

                sb.append("[");
                String tableName = rs.getMetaData().getTableName(i);
                if (StringUtils.isNotEmpty(tableName)) {
                    sb.append(tableName).append('.');
                }
                sb.append(key + "->" + val + "]");
            }
            System.out.println(sb.toString());
        }
    }
}
