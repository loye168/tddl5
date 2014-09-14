package com.alibaba.cobar;

/**
 * Cobar报警关键词定义
 * 
 * @author xianmao.hexm 2012-4-19
 */
public interface CobarAlarms {

    /** 默认报警关键词 **/
    String DEFAULT           = "#!TDDL#";

    /** 集群无有效的节点可提供服务 **/
    String CLUSTER_EMPTY     = "#!CLUSTER_EMPTY#";

    /** 数据节点的数据源发生切换 **/
    String DATANODE_SWITCH   = "#!DN_SWITCH#";

    /** 隔离区非法用户访问 **/
    String QUARANTINE_ATTACK = "#!QT_ATTACK#";

}
