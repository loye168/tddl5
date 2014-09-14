package com.alibaba.cobar.manager.util;

/**
 * Cobar Manager执行的查询语句，以及字段名
 */
public interface SQLDefine {

    /**
     * 查询语句
     */
    String SHOW_TIME_CURRENT        = "show @@time.current";
    String SHOW_TIME_STARTUP        = "show @@time.startup";
    String SHOW_VERSION             = "show @@version";
    String SHOW_SERVER              = "show @@server";
    String SHOW_PROCESSOR           = "show @@processor";
    String SHOW_THREADPOOL          = "show @@threadpool";
    String SHOW_DATABASE            = "show @@database";
    /** 1.2.2 **/
    String SHOW_DATABASES           = "show @@databases";
    String SHOW_DATANODE            = "show @@dataNode";
    /** 1.2.2 **/
    String SHOW_DATANODES           = "show @@dataNodes";
    String SHOW_DATANODE_WHERE      = "show @@dataNode where schema = ";
    String SHOW_DATASOURCE          = "show @@dataSource";
    /** 1.2.2 **/
    String SHOW_DATASOURCES         = "show @@dataSources";
    String SHOW_DATASOURCE_WHERE    = "show @@dataSource where datanode = ";
    String SHOW_CONNECTIONPOOL      = "show @@connectionpool";
    String SHOW_CONNECTION          = "show @@connection";
    String SHOW_CONNECTION_SQL      = "show @@connection.sql";
    String SHOW_COMMAND             = "show @@command";
    String SHOW_SQL_EXECUTE         = "show @@sql.execute";
    String SHOW_SQL_DETAIL          = "show @@sql.detail where id=";
    String SHOW_SQL                 = "show @@sql where id=";
    String SHOW_SQL_SLOW            = "show @@sql.slow";
    String SHOW_PARSER              = "show @@parser";
    String SHOW_ROUTER              = "show @@router";

    /**
     * 控制语句
     */
    String SWITCH_DATASOURCE        = "switch @@dataSource ";
    String KILL_CONNECTION          = "kill @@connection ";
    String STOP_HEARTBEAT           = "stop @@heartbeat ";
    String RELOAD_CONFIG            = "reload @@config";
    String RELOAD_ROUTE             = "reload @@route";
    String RELOAD_USER              = "reload @@user";
    String ROLLBACK_CONFIG          = "rollback @@config";
    String ROLLBACK_ROUTE           = "rollback @@route";
    String ROLLBACK_USER            = "rollback @@user";
    String ONLINE                   = "online";
    String OFFLINE                  = "offline";

    /**
     * 字段
     */
    /** show @@time.[current][startup] */
    String TIMESTAMP                = "TIMESTAMP";

    /** show @@version */
    String VERSION                  = "VERSION";

    /** show @@server */
    String UPTIME                   = "UPTIME";
    String STATUS                   = "STATUS";
    String RELOAD_TIME              = "RELOAD_TIME";
    String ROLLBACK_TIME            = "ROLLBACK_TIME";
    String USED_MEMORY              = "USED_MEMORY";
    String TOTAL_MEMORY             = "TOTAL_MEMORY";
    String MAX_MEMORY               = "MAX_MEMORY";
    String S_CHARSET                = "CHARSET";

    /** show @@processor */
    String P_NAME                   = "NAME";
    String P_NET_IN                 = "NET_IN";
    String P_NET_OUT                = "NET_OUT";
    String R_QUEUE                  = "R_QUEUE";
    String REQUEST_COUNT            = "REQUEST_COUNT";
    String REACT_COUNT              = "REACT_COUNT";
    String W_QUEUE                  = "W_QUEUE";
    String FC_COUNT                 = "FC_COUNT";
    String BC_COUNT                 = "BC_COUNT";
    String CONNECTIONS              = "CONNECTIONS";
    String FREE_BUFFER              = "FREE_BUFFER";
    String TOTAL_BUFFER             = "TOTAL_BUFFER";

    /** show @@threadpool */
    String TP_NAME                  = "NAME";
    String POOL_SIZE                = "POOL_SIZE";
    String ACTIVE_COUNT             = "ACTIVE_COUNT";
    String TASK_QUEUE_SIZE          = "TASK_QUEUE_SIZE";
    String COMPLETED_TASK           = "COMPLETED_TASK";
    String TOTAL_TASK               = "TOTAL_TASK";

    /** show @@databases */
    String DATABASE                 = "DATABASE";

    /** show @@datanodes */
    String POOL_NAME                = "NAME";
    String DS                       = "DATASOURCES";
    String TYPE                     = "TYPE";
    String INDEX                    = "INDEX";
    String ACTIVE                   = "ACTIVE";
    String IDLE                     = "IDLE";
    String SIZE                     = "SIZE";
    String EXECUTE                  = "EXECUTE";
    String TOTAL_TIME               = "TOTAL_TIME";
    String MAX_TIME                 = "MAX_TIME";
    String MAX_SQL                  = "MAX_SQL";
    String RECOVERY_TIME            = "RECOVERY_TIME";

    /** show @@datasources */
    String DS_SCHEMA                = "SCHEMA";
    String DS_NAME                  = "NAME";
    String DS_GROUP                 = "GROUP";
    String DS_URL                   = "URL";
    String DS_USER                  = "USER";
    String DS_TYPE                  = "TYPE";
    String DS_INIT                  = "INIT";
    String DS_MIN                   = "MIN";
    String DS_MAX                   = "MAX";
    String DS_IDLE                  = "IDLE_TIMEOUT";
    String DS_MAX_WAIT              = "MAX_WAIT";
    String DS_ACTIVE                = "ACTIVE_COUNT";
    String DS_POOLING               = "POOLING_COUNT";

    /** show @@connection */
    String C_PROCESSOR              = "PROCESSOR";
    String ID                       = "ID";
    String HOST                     = "HOST";
    String PORT                     = "PORT";
    String CHARSET                  = "CHARSET";
    String C_NET_IN                 = "NET_IN";
    String C_NET_OUT                = "NET_OUT";
    String ALIVE_TIME               = "ALIVE_TIME(S)";
    String ATTEMPS_COUNT            = "WRITE_ATTEMPTS";
    String RECV_BUFFER              = "RECV_BUFFER";
    String SEND_QUEUE               = "SEND_QUEUE";
    String LOCAL_PORT               = "LOCAL_PORT";
    String SCHEMA                   = "SCHEMA";
    String CHENNEL                  = "CHANNELS";

    /** show @@command */
    String CMD_PROCESSOR            = "PROCESSOR";
    String INIT_DB                  = "INIT_DB";
    String QUERY                    = "QUERY";
    String STMT_PREPARED            = "STMT_PREPARE";
    String STMT_EXECUTE             = "STMT_EXECUTE";
    String STMT_CLOSE               = "STMT_CLOSE";
    String PING                     = "PING";
    String KILL                     = "KILL";
    String QUIT                     = "QUIT";
    String OTHER                    = "OTHER";

    /** show @@sql.execute */
    String E_SQL                    = "SQL_ID";
    String E_EXECUTE                = "EXECUTE";
    String E_TIME                   = "TIME";
    String E_MAX_TIME               = "MAX_TIME";
    String E_MIN_TIME               = "MIN_TIME";

    /** show @@sql.detail where id =? */
    String D_DATA_SOURCE            = "DATA_SOURCE";
    String D_EXECUTE                = "EXECUTE";
    String D_TIME                   = "TIME";
    String D_LAST_EXECUTE_TIMESTAMP = "LAST_EXECUTE_TIMESTAMP";
    String D_LAST_TIME              = "LAST_TIME";

    /** show @@sql where id =? */
    String SQL_ID                   = "SQL_ID";
    String SQL_DETAIL               = "SQL_DETAIL";

    /** show @@sql.slow */
    String S_TIME                   = "TIME";
    String S_DATA_SOURCE            = "DATA_SOURCE";
    String S_EXECUTE_TIMESTAMP      = "EXECUTE_TIMESTAMP";
    String S_SQL                    = "SQL";

    /** show @@connection.sql */
    String CS_ID                    = "ID";
    String CS_HOST                  = "HOST";
    String EXECUTE_START            = "EXECUTE_START";
    String EXECUTE_TIME             = "EXECUTE_TIME(S)";
    String EXECUTE_SQL              = "EXECUTE_SQL";

    /** show @@parser */
    String PROCESSOR_NAME           = "PROCESSOR_NAME";
    String PARSE_COUNT              = "PARSE_COUNT";
    String TIME_COUNT               = "TIME_COUNT";
    String MAX_PARSE_TIME           = "MAX_PARSE_TIME";
    String MAX_PARSE_SQL_ID         = "MAX_PARSE_SQL_ID";
    String CACHED_COUNT             = "CHEMA_COUNT";
    String CACHE_SIZE               = "CACHE_SIZE";

    /** show @@router */
    String R_P_NAME                 = "PROCESSOR_NAME";
    String ROUTE_COUNT              = "ROUTE_COUNT";
    String R_TIME_COUNT             = "TIME_COUNT";
    String MAX_ROUTE_TIME           = "MAX_ROUTE_TIME";
    String MAX_ROUTE_SQL_ID         = "MAX_ROUTE_SQL_ID";

}
