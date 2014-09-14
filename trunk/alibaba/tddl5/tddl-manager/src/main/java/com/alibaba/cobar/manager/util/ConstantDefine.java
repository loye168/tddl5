package com.alibaba.cobar.manager.util;

/**
 * @author haiqing.zhuhq 2011-8-5
 */
public interface ConstantDefine {

    /**
     * 与Cobar Server通信类型
     */
    public static final int    TYPE_SERVER_STATUS              = 1;
    public static final int    TYPE_PROCESSOR_STATUS           = 2;
    public static final int    TYPE_CONNECTION                 = 3;
    public static final int    TYPE_PARSER                     = 4;
    public static final int    TYPE_ROUTER                     = 5;
    public static final int    TYPE_THREAD_POOL                = 6;
    public static final int    TYPE_COMMAND                    = 7;
    public static final int    TYPE_COMMAND_TPS                = 8;
    public static final int    TYPE_DATANODES                  = 9;
    public static final int    TYPE_CONNECTION_LIST            = 10;
    public static final int    TYPE_DATABASES                  = 11;
    public static final int    TYPE_DATASOURCES                = 12;

    /**
     * Cobar List页面查询的类型
     */
    public static final int    TYPE_COBAR_MEMORY_USAGE         = 1;
    public static final int    TYPE_INDEX                      = 2;
    public static final int    TYPE_CLUSTER_INFO               = 3;
    public static final int    TYPE_CLUSTER_THROUGHPUT_INFO    = 4;
    public static final int    TYPE_STATUS                     = 5;

    /**
     * Cobar Manager系统管理部分的ajax类型
     */
    public static final int    CLUSTER_NAME_REPEAT             = 1;
    public static final int    CHANGE_ROLE                     = 2;
    public static final int    CLUSTER_NAME_REPEAT_EXCEPT_SELF = 3;
    public static final int    CHECK_OLD_PWD                   = 4;
    public static final int    CHECK_USER_NAME_REPEAT          = 5;
    public static final int    COBAR_NAME_REPEAT               = 6;
    public static final int    COBAR_NAME_REPEAT_EXCEPT_SELF   = 7;
    public static final int    USER_NAME_REPEAT_EXCEPT_SELF    = 8;
    public static final int    USER_NAME_REPEAT                = 9;
    public static final int    STOP_TIME_REPEAT                = 10;
    public static final int    DELETE_STOP_TIME                = 11;
    public static final int    ADD_STOP_TIME                   = 12;
    public static final int    BU_NAME_REPEAT                  = 13;
    public static final int    ADD_VIP                         = 14;
    public static final int    DELETE_VIP                      = 15;
    public static final int    VIP_NAME_REPEAT                 = 16;
    public static final int    PASSWORD_VALIDATE               = 17;

    /**
     * Cobar Manager Control
     */
    public static final int    CONFIG_RELOAD                   = 3;
    public static final int    CONFIG_ROLLBACK                 = 4;
    public static final int    KILL_CONNECTION                 = 5;
    public static final int    COBAR_LIST                      = 6;
    public static final int    SWITCH_DATABASE                 = 7;
    public static final int    STOP_HEARTBEAT                  = 8;
    public static final int    RECOVERY_HEARTBEAT              = 9;
    public static final int    GET_DATANODES_INDEX             = 10;

    /**
     * Alert Control
     */
    public static final int    UNKNOW                          = 0;
    public static final int    CHOOSE_COBAR                    = 1;
    public static final int    CONNECTION_FAIL                 = 2;
    public static final int    DATANODE_DIFF                   = 3;
    public static final int    LOGIN                           = 4;
    public static final int    USER_NULL                       = 5;
    public static final int    PASSWORD_NULL                   = 6;
    public static final int    CHOOSE_DATANODE                 = 7;

    /*
     * cobar status
     */
    public static final String ACTIVE                          = "Active";
    public static final String IN_ACTIVE                       = "InActive";
    public static final String DELETE                          = "Delete";
    public static final String ERROR                           = "Error";

    /*
     * user status
     */
    public static final String FORBIDDEN                       = "Forbidden";
    public static final String NORMAL                          = "Normal";

    /*
     * user type
     */
    public static final String SYSTEM_ADMIN                    = "System_Admin";
    public static final String CLUSTER_ADMIN                   = "Cluster_Admin";

    /*
     * for xml read
     */
    public static final int    ID                              = 1;
    public static final int    NAME                            = 2;
    public static final int    STATUS                          = 3;

    public static final int    SORT_ID                         = 4;
    public static final int    DEPLOY_CONTACT                  = 5;
    public static final int    MAINT_CONTACT                   = 6;
    public static final int    DEPLOY_DESC                     = 7;
    public static final int    ONLINE_TIME                     = 8;

    public static final int    CLUSTER_ID                      = 9;
    public static final int    HOST                            = 10;
    public static final int    PORT                            = 11;
    public static final int    USER                            = 12;
    public static final int    PASSWORD                        = 13;
    public static final int    TIME_DIFF                       = 14;

    public static final int    REALNAME                        = 15;
    public static final int    USERNAME                        = 16;
    public static final int    USER_ROLE                       = 17;

    public static final int    SID                             = 18;
    public static final int    COBAR_IDS                       = 19;

    public static final int    SERVER_PORT                     = 20;
    public static final int    SCHEMA                          = 21;
    public static final int    WEIGHT                          = 22;

    public static final int    ENV                             = 23;

}
