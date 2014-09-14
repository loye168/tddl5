package com.alibaba.cobar;

/**
 * 命令类别定义
 * 
 * @author xianmao.hexm
 */
public interface Commands {

    // none, this is an internal thread state
    byte COM_SLEEP               = 0;

    // mysql_close
    byte COM_QUIT                = 1;

    // mysql_select_db
    byte COM_INIT_DB             = 2;

    // mysql_real_query
    byte COM_QUERY               = 3;

    // mysql_list_fields
    byte COM_FIELD_LIST          = 4;

    // mysql_create_db (deprecated)
    byte COM_CREATE_DB           = 5;

    // mysql_drop_db (deprecated)
    byte COM_DROP_DB             = 6;

    // mysql_refresh
    byte COM_REFRESH             = 7;

    // mysql_shutdown
    byte COM_SHUTDOWN            = 8;

    // mysql_stat
    byte COM_STATISTICS          = 9;

    // mysql_list_processes
    byte COM_PROCESS_INFO        = 10;

    // none, this is an internal thread state
    byte COM_CONNECT             = 11;

    // mysql_kill
    byte COM_PROCESS_KILL        = 12;

    // mysql_dump_debug_info
    byte COM_DEBUG               = 13;

    // mysql_ping
    byte COM_PING                = 14;

    // none, this is an internal thread state
    byte COM_TIME                = 15;

    // none, this is an internal thread state
    byte COM_DELAYED_INSERT      = 16;

    // mysql_change_user
    byte COM_CHANGE_USER         = 17;

    // used by slave server mysqlbinlog
    byte COM_BINLOG_DUMP         = 18;

    // used by slave server to get master table
    byte COM_TABLE_DUMP          = 19;

    // used by slave to log connection to master
    byte COM_CONNECT_OUT         = 20;

    // used by slave to register to master
    byte COM_REGISTER_SLAVE      = 21;

    // mysql_stmt_prepare
    byte COM_STMT_PREPARE        = 22;

    // mysql_stmt_execute
    byte COM_STMT_EXECUTE        = 23;

    // mysql_stmt_send_long_data
    byte COM_STMT_SEND_LONG_DATA = 24;

    // mysql_stmt_close
    byte COM_STMT_CLOSE          = 25;

    // mysql_stmt_reset
    byte COM_STMT_RESET          = 26;

    // mysql_set_server_option
    byte COM_SET_OPTION          = 27;

    // mysql_stmt_fetch
    byte COM_STMT_FETCH          = 28;

}
