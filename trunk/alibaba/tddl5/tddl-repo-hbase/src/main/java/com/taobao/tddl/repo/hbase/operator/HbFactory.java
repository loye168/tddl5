package com.taobao.tddl.repo.hbase.operator;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;

/**
 * @author jianghang 2013-7-26 下午1:48:03
 */
public class HbFactory extends AbstractLifecycle {

    private Map<String, String> clusterConfig;
    private Configuration       conficuration = null;
    private HTablePool          tablePool     = null;
    private String              clusterName   = null;

    public HbFactory(){

    }

    public HbFactory(Map<String, String> clusterConfig){
        this.clusterConfig = clusterConfig;
    }

    @Override
    public void doInit() {
        if (null == clusterConfig) {
            throw new IllegalArgumentException("missing cluster config for HbFactory.");
        }

        initConfiguration();
    }

    @Override
    public void doDestroy() {
        if (tablePool != null) {
            try {
                tablePool.close();
            } catch (IOException e) {
                throw new TddlNestableRuntimeException(e);
            }
        }
    }

    private void initConfiguration() {
        if (clusterConfig.get(HbaseConf.cluster_name) == null || "".equals(clusterConfig.get(HbaseConf.cluster_name))) {
            throw new IllegalArgumentException("cluster name can not be null or ''!");
        }

        clusterName = clusterConfig.get(HbaseConf.cluster_name);
        Configuration conf = HBaseConfiguration.create();
        conf.set(HbaseConf.hbase_quorum, clusterConfig.get(HbaseConf.hbase_quorum));
        conf.set(HbaseConf.hbase_clientPort, clusterConfig.get(HbaseConf.hbase_clientPort));
        if (null != clusterConfig.get(HbaseConf.hbase_znode_parent)) {
            conf.set(HbaseConf.hbase_znode_parent, clusterConfig.get(HbaseConf.hbase_znode_parent));
        }

        conf.set("hbase.client.retries.number", "5");
        conf.set("hbase.client.pause", "200");
        conf.set("ipc.ping.interval", "3000");
        conf.setBoolean("hbase.ipc.client.tcpnodelay", true);

        if (this.checkConfiguration(clusterConfig.get(HbaseConf.cluster_name), conf)) {
            conficuration = conf;
            tablePool = new HTablePool(conf, 100);

        }
    }

    /**
     * 检查下Hmaster是否启动
     */
    private boolean checkConfiguration(String name, Configuration configuration) {
        try {
            HBaseAdmin.checkHBaseAvailable(configuration);
        } catch (MasterNotRunningException e) {
            throw new TddlNestableRuntimeException("connect to master fail with cluster name(" + name
                                                   + "),configuration("
                                                   + ToStringBuilder.reflectionToString(configuration) + ")", e);
        } catch (ZooKeeperConnectionException e) {
            throw new TddlNestableRuntimeException("connect to zookeeper fail with cluster name(" + name
                                                   + "), configuration("
                                                   + ToStringBuilder.reflectionToString(configuration) + ")", e);
        }
        return true;
    }

    /**
     * @param cluster
     * @return
     */
    public HBaseAdmin getHbaseAdmin() {
        if (null == conficuration) {
            throw new IllegalArgumentException("get hbase admin fail with not exist cluster,or not inited");
        }

        try {
            return new HBaseAdmin(conficuration);
        } catch (MasterNotRunningException e) {
            throw new TddlNestableRuntimeException("get hbase admin fail when connect to master with cluster("
                                                   + clusterName + "),configuration("
                                                   + ToStringBuilder.reflectionToString(conficuration) + ")", e);
        } catch (ZooKeeperConnectionException e) {
            throw new TddlNestableRuntimeException("get hbase admin fail when connect to zookeeper with cluster("
                                                   + clusterName + "),configuration("
                                                   + ToStringBuilder.reflectionToString(conficuration) + ")", e);
        }
    }

    /**
     * @param cluster
     * @param tableName
     * @return
     */
    public HTableInterface getHtable(String tableName) {
        try {
            return tablePool.getTable(tableName);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException("get htable admin fail with htable(" + tableName + "),cluster("
                                                   + clusterName + "),configuration("
                                                   + ToStringBuilder.reflectionToString(conficuration) + ")", e);
        }
    }

    public void setClusterConfig(Map<String, String> clusterConfig) {
        this.clusterConfig = clusterConfig;
    }

    public String getClusterName() {
        return clusterName;
    }

    public static class HbaseConf {

        public static final String cluster_name       = "cluster_name";
        public static final String hbase_quorum       = "hbase.zookeeper.quorum";
        public static final String hbase_clientPort   = "hbase.zookeeper.property.clientPort";
        public static final String hbase_znode_parent = "zookeeper.znode.parent";
    }
}
