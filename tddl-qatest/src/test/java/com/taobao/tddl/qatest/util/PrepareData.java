package com.taobao.tddl.qatest.util;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.qatest.BaseTestCase;

public class PrepareData extends BaseTestCase {

    /**
     * normaltbl表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void normaltblPrepare(int start, int end) throws Exception, SQLException {
        tddlUpdateData("delete from " + normaltblTableName, null);
        mysqlUpdateData("delete from  " + normaltblTableName, null);

        String sql = "REPLACE INTO " + normaltblTableName
                     + " (pk,id,gmt_create,gmt_timestamp,gmt_datetime,name,floatCol) VALUES(?,?,?,?,?,?,?)";
        mysqlConnection.setAutoCommit(false);
        mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
        tddlPreparedStatement = tddlConnection.prepareStatement(sql);
        for (int i = start; i < end / 2; i++) {
            tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
            mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
            tddlPreparedStatement.setObject(2, i % 4 * 100);
            mysqlPreparedStatement.setObject(2, i % 4 * 100);
            tddlPreparedStatement.setObject(3, gmtDay);
            mysqlPreparedStatement.setObject(3, gmtDay);
            tddlPreparedStatement.setObject(4, gmt);
            mysqlPreparedStatement.setObject(4, gmt);
            tddlPreparedStatement.setObject(5, gmt);
            mysqlPreparedStatement.setObject(5, gmt);
            tddlPreparedStatement.setObject(6, name);
            mysqlPreparedStatement.setObject(6, name);
            tddlPreparedStatement.setObject(7, 1.1);
            mysqlPreparedStatement.setObject(7, 1.1);
            tddlPreparedStatement.execute();
            mysqlPreparedStatement.addBatch();
        }

        for (int i = end / 2; i < end - 1; i++) {
            tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
            mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
            tddlPreparedStatement.setObject(2, i * 100);
            mysqlPreparedStatement.setObject(2, i * 100);
            tddlPreparedStatement.setObject(3, gmtDayNext);
            mysqlPreparedStatement.setObject(3, gmtDayNext);
            tddlPreparedStatement.setObject(4, gmtNext);
            mysqlPreparedStatement.setObject(4, gmtNext);
            tddlPreparedStatement.setObject(5, gmtNext);
            mysqlPreparedStatement.setObject(5, gmtNext);
            tddlPreparedStatement.setObject(6, newName);
            mysqlPreparedStatement.setObject(6, newName);
            tddlPreparedStatement.setObject(7, 1.1);
            mysqlPreparedStatement.setObject(7, 1.1);
            tddlPreparedStatement.execute();
            mysqlPreparedStatement.addBatch();
        }

        for (int i = end - 1; i < end; i++) {
            tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
            mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
            tddlPreparedStatement.setObject(2, i * 100);
            mysqlPreparedStatement.setObject(2, i * 100);
            tddlPreparedStatement.setObject(3, gmtDayBefore);
            mysqlPreparedStatement.setObject(3, gmtDayBefore);
            tddlPreparedStatement.setObject(4, gmtBefore);
            mysqlPreparedStatement.setObject(4, gmtBefore);
            tddlPreparedStatement.setObject(5, gmtBefore);
            mysqlPreparedStatement.setObject(5, gmtBefore);
            tddlPreparedStatement.setObject(6, name1);
            mysqlPreparedStatement.setObject(6, name1);
            tddlPreparedStatement.setObject(7, (float) (i * 0.01));
            mysqlPreparedStatement.setObject(7, (float) (i * 0.01));
            tddlPreparedStatement.execute();
            mysqlPreparedStatement.addBatch();
        }
        mysqlPreparedStatement.executeBatch();
        mysqlConnection.commit();

    }

    public void demoRepoPrepare(int start, int end) throws Exception, SQLException {
        tddlUpdateData("delete from _tddl_", null);
        mysqlUpdateData("delete from _tddl_", null);

        String sql = "REPLACE INTO _tddl_ (id,name) VALUES(?,?)";
        mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
        tddlPreparedStatement = tddlConnection.prepareStatement(sql);

        for (int i = start; i < end; i++) {
            mysqlPreparedStatement.setObject(1, i);
            tddlPreparedStatement.setObject(1, i);

            mysqlPreparedStatement.setObject(2, "sun" + i);
            tddlPreparedStatement.setObject(2, "sun" + i);

            mysqlPreparedStatement.execute();
            tddlPreparedStatement.execute();
        }

    }

    /**
     * normaltbl表数据的准备 start为插入起始数据，end为插入结束数据，部分name字段插入数据为null
     */
    public void normaltblNullPrepare(int start, int end) throws Exception, SQLException {
        tddlUpdateData("delete from " + normaltblTableName, null);
        mysqlUpdateData("delete from  " + normaltblTableName, null);

        String sql = "REPLACE INTO " + normaltblTableName
                     + " (pk,id,gmt_create,gmt_timestamp,gmt_datetime,name,floatCol) VALUES(?,?,?,?,?,?,?)";
        mysqlConnection.setAutoCommit(false);
        mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
        PreparedStatement andorPs = tddlConnection.prepareStatement(sql);
        for (int i = start; i < end / 2; i++) {
            andorPs.setObject(1, Long.parseLong(i + ""));
            mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
            andorPs.setObject(2, i * 100);
            mysqlPreparedStatement.setObject(2, i * 100);
            andorPs.setObject(3, gmtDay);
            mysqlPreparedStatement.setObject(3, gmtDay);
            andorPs.setObject(4, gmt);
            mysqlPreparedStatement.setObject(4, gmt);
            andorPs.setObject(5, gmt);
            mysqlPreparedStatement.setObject(5, gmt);
            andorPs.setObject(6, name);
            mysqlPreparedStatement.setObject(6, name);
            andorPs.setObject(7, 1.1);
            mysqlPreparedStatement.setObject(7, 1.1);
            andorPs.execute();
            mysqlPreparedStatement.addBatch();
        }

        for (int i = end / 2; i < end - 1; i++) {
            andorPs.setObject(1, Long.parseLong(i + ""));
            mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
            andorPs.setObject(2, i * 100);
            mysqlPreparedStatement.setObject(2, i * 100);
            andorPs.setObject(3, gmtNext);
            mysqlPreparedStatement.setObject(3, gmtNext);
            andorPs.setObject(4, gmtNext);
            mysqlPreparedStatement.setObject(4, gmtNext);
            andorPs.setObject(5, gmtNext);
            mysqlPreparedStatement.setObject(5, gmtNext);
            andorPs.setObject(6, null);
            mysqlPreparedStatement.setObject(6, null);
            andorPs.setObject(7, 1.1);
            mysqlPreparedStatement.setObject(7, 1.1);
            andorPs.execute();
            mysqlPreparedStatement.addBatch();
        }

        for (int i = end - 1; i < end; i++) {
            andorPs.setObject(1, Long.parseLong(i + ""));
            mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
            andorPs.setObject(2, i * 100);
            mysqlPreparedStatement.setObject(2, i * 100);
            andorPs.setObject(3, gmtBefore);
            mysqlPreparedStatement.setObject(3, gmtBefore);
            andorPs.setObject(4, gmtBefore);
            mysqlPreparedStatement.setObject(4, gmtBefore);
            andorPs.setObject(5, gmtBefore);
            mysqlPreparedStatement.setObject(5, gmtBefore);
            andorPs.setObject(6, name1);
            mysqlPreparedStatement.setObject(6, name1);
            andorPs.setObject(7, (float) (i * 0.01));
            mysqlPreparedStatement.setObject(7, (float) (i * 0.01));
            andorPs.execute();
            mysqlPreparedStatement.addBatch();
        }
        mysqlPreparedStatement.executeBatch();
        mysqlConnection.commit();

    }

    /**
     * hostinfo表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void hostinfoPrepare(int start, int end) throws Exception, SQLException {
        tddlUpdateData("delete from " + host_info, null);
        mysqlUpdateData("delete from  " + host_info, null);
        try {
            String sql = "replace into " + host_info + "(host_id,host_name,hostgroup_id,station_id) values(?,?,?,?)";
            mysqlConnection.setAutoCommit(false);
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);

            for (int i = start; i < end; i++) {
                tddlPreparedStatement = tddlConnection.prepareStatement(sql);

                mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(2, "hostname" + i);
                tddlPreparedStatement.setObject(2, "hostname" + i);
                mysqlPreparedStatement.setObject(3, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(3, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(4, "station" + i / 2);
                tddlPreparedStatement.setObject(4, "station" + i / 2);
                tddlPreparedStatement.execute();
                mysqlPreparedStatement.addBatch();
            }
            mysqlPreparedStatement.executeBatch();
            mysqlConnection.commit();
        } catch (Exception ex) {
            throw new TddlNestableRuntimeException(ex);
        } finally {

        }
    }

    /**
     * 插入hostinfo表数据，其中start为插入起始数据，end为插入结束数据 groupidValue为字段hostgroup_id的值
     */
    public void hostinfoDataAdd(int start, int end, long groupidValue) throws Exception, SQLException {
        try {
            String sql = "replace into " + host_info + "(host_id,host_name,hostgroup_id,station_id) values(?,?,?,?)";
            mysqlConnection.setAutoCommit(false);
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);

            for (int i = start; i < end; i++) {
                tddlPreparedStatement = tddlConnection.prepareStatement(sql);

                mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(2, "hostname" + i);
                tddlPreparedStatement.setObject(2, "hostname" + i);
                mysqlPreparedStatement.setObject(3, groupidValue);
                tddlPreparedStatement.setObject(3, groupidValue);
                mysqlPreparedStatement.setObject(4, "station" + i / 2);
                tddlPreparedStatement.setObject(4, "station" + i / 2);
                tddlPreparedStatement.execute();
                mysqlPreparedStatement.addBatch();
            }
            mysqlPreparedStatement.executeBatch();
            mysqlConnection.commit();
        } catch (Exception ex) {
            throw new TddlNestableRuntimeException(ex);
        } finally {

        }
    }

    public void hostgroupPrepare(int start, int end) throws Exception, SQLException {
        tddlUpdateData("delete from " + hostgroup, null);
        mysqlUpdateData("delete from  " + hostgroup, null);
        try {
            String sql = "replace into " + hostgroup
                         + "(hostgroup_id,hostgroup_name,module_id,station_id) values(?,?,?,?)";
            mysqlConnection.setAutoCommit(false);
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
            for (int i = start; i < end; i++) {
                tddlPreparedStatement = tddlConnection.prepareStatement(sql);
                mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(2, "hostgroupname" + i);
                tddlPreparedStatement.setObject(2, "hostgroupname" + i);
                mysqlPreparedStatement.setObject(3, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(3, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(4, "station" + i / 2);
                tddlPreparedStatement.setObject(4, "station" + i / 2);

                mysqlPreparedStatement.addBatch();
                tddlPreparedStatement.execute();
            }
            mysqlPreparedStatement.executeBatch();
            mysqlConnection.commit();
        } catch (Exception ex) {
            throw new TddlNestableRuntimeException(ex);
        } finally {

        }
    }

    /**
     * 插入hostinfo表数据 start为插入起始数据，end为插入结束数据 其中moduleIdValue为字段module_id的值
     */
    public void hostgroupDataAdd(int start, int end, long moduleIdValue) throws Exception, SQLException {
        try {
            String sql = "replace into " + hostgroup
                         + "(hostgroup_id,hostgroup_name,module_id,station_id) values(?,?,?,?)";
            mysqlConnection.setAutoCommit(false);
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
            for (int i = start; i < end; i++) {
                tddlPreparedStatement = tddlConnection.prepareStatement(sql);
                mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(2, "hostgroupname" + i);
                tddlPreparedStatement.setObject(2, "hostgroupname" + i);
                mysqlPreparedStatement.setObject(3, moduleIdValue);
                tddlPreparedStatement.setObject(3, moduleIdValue);
                mysqlPreparedStatement.setObject(4, "station" + i / 2);
                tddlPreparedStatement.setObject(4, "station" + i / 2);
                mysqlPreparedStatement.addBatch();
                tddlPreparedStatement.execute();
            }
            mysqlPreparedStatement.executeBatch();
            mysqlConnection.commit();
        } catch (Exception ex) {
            throw new TddlNestableRuntimeException(ex);
        } finally {

        }
    }

    /**
     * hostgroupInfo表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void hostgroupInfoPrepare(int start, int end) throws Exception, SQLException {
        tddlUpdateData("delete from " + hostgroup_info, null);
        mysqlUpdateData("delete from  " + hostgroup_info, null);
        try {
            String sql = "replace into " + hostgroup_info + "(hostgroup_id,hostgroup_name,station_id) values(?,?,?)";
            mysqlConnection.setAutoCommit(false);
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
            for (int i = start; i < end; i++) {
                tddlPreparedStatement = tddlConnection.prepareStatement(sql);
                mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(2, "hostgroupname" + i);
                tddlPreparedStatement.setObject(2, "hostgroupname" + i);
                mysqlPreparedStatement.setObject(3, "station" + i / 2);
                tddlPreparedStatement.setObject(3, "station" + i / 2);
                mysqlPreparedStatement.addBatch();
                tddlPreparedStatement.execute();
            }

            mysqlPreparedStatement.executeBatch();
            mysqlConnection.commit();
        } catch (Exception ex) {
            throw new TddlNestableRuntimeException(ex);
        } finally {

        }
    }

    /**
     * module_info表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void module_infoPrepare(int start, int end) throws Exception, SQLException {
        tddlUpdateData("delete from " + module_info, null);
        mysqlUpdateData("delete from  " + module_info, null);
        try {
            String sql = "replace into " + module_info + "(module_id,product_id,module_name) values(?,?,?)";
            mysqlConnection.setAutoCommit(false);
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
            for (int i = 0; i < end; i++) {
                tddlPreparedStatement = tddlConnection.prepareStatement(sql);
                mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(2, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(2, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(3, "module" + i);
                tddlPreparedStatement.setObject(3, "module" + i);
                mysqlPreparedStatement.addBatch();
                tddlPreparedStatement.execute();
            }
            mysqlPreparedStatement.executeBatch();
            mysqlConnection.commit();
        } catch (Exception ex) {
            throw new TddlNestableRuntimeException(ex);
        } finally {
        }
    }

    /**
     * module_host表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void module_hostPrepare(int start, int end) throws Exception, SQLException {
        tddlUpdateData("delete from " + module_host, null);
        mysqlUpdateData("delete from  " + module_host, null);
        try {
            String sql = "replace into " + module_host + "(id,module_id,host_id) values(?,?,?)";
            mysqlConnection.setAutoCommit(false);
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
            for (int i = start; i < end; i++) {
                tddlPreparedStatement = tddlConnection.prepareStatement(sql);
                mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(2, Long.parseLong(i / 2 + ""));
                tddlPreparedStatement.setObject(2, Long.parseLong(i / 2 + ""));
                mysqlPreparedStatement.setObject(3, Long.parseLong(i % 3 + ""));
                tddlPreparedStatement.setObject(3, Long.parseLong(i % 3 + ""));
                mysqlPreparedStatement.addBatch();
                tddlPreparedStatement.execute();
            }
            mysqlPreparedStatement.executeBatch();
            mysqlConnection.commit();
        } catch (Exception ex) {
            throw new TddlNestableRuntimeException(ex);
        } finally {
        }
    }

    /**
     * 为like测试准备单独的2条normaltbl表数据
     */
    public void normaltblTwoPrepare() throws Exception {
        String sql = "REPLACE INTO " + normaltblTableName + "(pk,name,gmt_create) VALUES(?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.clear();
        param.add(20l);
        param.add(name1);
        param.add(gmtDay);
        tddlUpdateData(sql, param);
        mysqlUpdateData(sql, param);

        sql = "REPLACE INTO " + normaltblTableName + "(pk,name,gmt_create) VALUES(?,?,?)";
        param.clear();
        param.add(21l);
        param.add(name2);
        param.add(gmtDay);
        tddlUpdateData(sql, param);
        mysqlUpdateData(sql, param);
    }

    /**
     * student表数据的准备 start为插入起始数据，end为插入结束数据
     */
    public void studentPrepare(int start, int end) throws Exception, SQLException {
        tddlUpdateData("delete from  " + studentTableName, null);
        mysqlUpdateData("delete from  " + studentTableName, null);
        try {
            String sql = "replace into  " + studentTableName + " (id,name) values(?,?)";
            mysqlConnection.setAutoCommit(false);
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
            for (int i = start; i < end; i++) {
                tddlPreparedStatement = tddlConnection.prepareStatement(sql);
                mysqlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                tddlPreparedStatement.setObject(1, Long.parseLong(i + ""));
                mysqlPreparedStatement.setObject(2, name);
                tddlPreparedStatement.setObject(2, name);
                mysqlPreparedStatement.addBatch();
                tddlPreparedStatement.execute();
            }
            mysqlPreparedStatement.executeBatch();
            mysqlConnection.commit();
        } catch (Exception ex) {
            throw new TddlNestableRuntimeException(ex);
        } finally {
        }
    }
}
