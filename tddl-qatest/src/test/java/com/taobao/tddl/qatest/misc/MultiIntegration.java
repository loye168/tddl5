package com.taobao.tddl.qatest.misc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Before;
import org.junit.Test;

import com.taobao.tddl.common.utils.thread.ExecutorTemplate;
import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;

/**
 * MultiIntegration
 * 
 * @author zhuoxue
 * @since 5.0.1
 */
public class MultiIntegration extends BaseMatrixTestCase {

    public MultiIntegration() throws Exception{
        BaseTestCase.host_info = "mysql_host_info_mutilGroup";
        BaseTestCase.module_info = "mysql_module_info_mutilGroup";
    }

    @Before
    public void prepare() throws Exception {
        hostinfoPrepare(0, 20);
        module_infoPrepare(0, 40);
    }

    /**
     * 条件为等于子查询返回的值
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void testCorrelated() {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        ExecutorTemplate template = new ExecutorTemplate(executor);

        int threadCount = 5;
        for (int i = 0; i < threadCount; i++) {
            template.submit(new Runnable() {

                @Override
                public void run() {
                    Connection mysqlConnection = null;
                    Connection tddlConnection = null;
                    try {
                        mysqlConnection = getConnection();
                        tddlConnection = tddlDatasource.getConnection();

                        for (int i = 0; i < 100; i++) {
                            String sql = "select * from " + host_info
                                         + " as host where host_id = (select module_id from " + module_info
                                         + " as info where host.hostgroup_id=info.module_id)";
                            String[] columnParam = { "host_id", "host_name", "hostgroup_id" };
                            try {
                                assertSame(sql, columnParam, null, mysqlConnection, tddlConnection);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    } catch (SQLException e1) {
                        e1.printStackTrace();
                    } finally {
                        try {
                            tddlConnection.close();
                            mysqlConnection.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        template.waitForResult();
    }

    /**
     * 子查询
     * 
     * @author zhuoxue
     * @since 5.0.1
     */
    @Test
    public void testSubquery() {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        ExecutorTemplate template = new ExecutorTemplate(executor);

        int threadCount = 5;
        for (int i = 0; i < threadCount; i++) {
            template.submit(new Runnable() {

                @Override
                public void run() {
                    Connection mysqlConnection = null;
                    Connection tddlConnection = null;
                    try {
                        mysqlConnection = getConnection();
                        tddlConnection = tddlDatasource.getConnection();

                        for (int i = 0; i < 100; i++) {
                            String sql = "select *  from " + host_info + " where hostgroup_id <(select module_id from "
                                         + module_info + " where module_name='module12')";
                            String[] columnParam = { "host_id", "host_name", "hostgroup_id" };
                            try {
                                assertSame(sql, columnParam, null, mysqlConnection, tddlConnection);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    } catch (SQLException e1) {
                        e1.printStackTrace();
                    } finally {
                        try {
                            tddlConnection.close();
                            mysqlConnection.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        template.waitForResult();
    }

    public void assertSame(String sql, String[] columnParam, List<Object> param, Connection mysqlConnection,
                           Connection tddlConnection) throws Exception {
        ResultSet rs = null;
        ResultSet rc = null;
        try {
            rs = mysqlQueryData(sql, param, mysqlConnection);
            rc = andorQueryData(sql, param, tddlConnection);
            assertContentSame(rs, rc, columnParam);
        } finally {
            rsRcClose(rs, rc);
        }
    }

    /**
     * mysql查询数据
     * 
     * @param sql
     * @param param
     * @return
     * @throws Exception
     */
    public ResultSet mysqlQueryData(String sql, List<Object> param, Connection mysqlConnection) throws Exception {
        ResultSet rs = null;
        try {
            PreparedStatement mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
            if (param == null) {
                rs = mysqlPreparedStatement.executeQuery();
            } else {
                for (int i = 0; i < param.size(); i++) {
                    mysqlPreparedStatement.setObject(i + 1, param.get(i));
                }
                rs = mysqlPreparedStatement.executeQuery();
            }
        } catch (Exception ex) {
            throw new Exception(ex);
        } finally {

        }
        return rs;
    }

    public ResultSet andorQueryData(String sql, List<Object> param, Connection tddlConnection) throws Exception {
        ResultSet rs = null;
        try {
            PreparedStatement tddlPreparedStatement = tddlConnection.prepareStatement(sql);
            if (param == null) {
                rs = tddlPreparedStatement.executeQuery();
            } else {
                for (int i = 0; i < param.size(); i++) {
                    tddlPreparedStatement.setObject(i + 1, param.get(i));
                }
                rs = tddlPreparedStatement.executeQuery();
            }
        } catch (Exception ex) {
            // tddlCon.rollback();
            if (!tddlConnection.getAutoCommit()) {
                tddlConnection.rollback();
            }
            throw new Exception(ex);
        } finally {

        }
        return rs;
    }
}
