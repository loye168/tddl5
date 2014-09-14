package com.alibaba.cobar.manager.dao;

import java.util.List;

import com.alibaba.cobar.manager.dataobject.cobarnode.CommandStatus;
import com.alibaba.cobar.manager.dataobject.cobarnode.ConnectionStatus;
import com.alibaba.cobar.manager.dataobject.cobarnode.DataNodesStatus;
import com.alibaba.cobar.manager.dataobject.cobarnode.DataSources;
import com.alibaba.cobar.manager.dataobject.cobarnode.ProcessorStatus;
import com.alibaba.cobar.manager.dataobject.cobarnode.ServerStatus;
import com.alibaba.cobar.manager.dataobject.cobarnode.ThreadPoolStatus;
import com.alibaba.cobar.manager.dataobject.cobarnode.TimeStamp;
import com.alibaba.cobar.manager.util.Pair;

/**
 * (created at 2010-7-26)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 * @author haiqing.zhuhq 2011-9-1
 */
public interface CobarAdapterDAO {

    /**
     * @return true if success. never throw Exception
     */
    boolean setCobarStatus(boolean status);

    TimeStamp getCurrentTime(); // show @@time.current

    TimeStamp getStartUpTime(); // show @@time.startup

    String getVersion(); // show @@version;

    ServerStatus getServerStatus(); // show @@server;

    List<ProcessorStatus> listProccessorStatus(); // show @@processor

    List<ThreadPoolStatus> listThreadPoolStatus(); // show @@threadpool

    List<String> listDataBases(); // show @@databases

    List<DataNodesStatus> listDataNodes(); // show @@dataNodes

    List<DataSources> listDataSources(); // show @@dataSources

    List<ConnectionStatus> listConnectionStatus(); // show @@connection

    List<CommandStatus> listCommandStatus(); // show @@command

    Pair<Long, Long> getCurrentTimeMillis();

    int switchDataNode(String datanodes, int index); // switch @@datasource
                                                     // datanodes:index

    int stopHeartbeat(String datanodes, int hour_time); // stop @@heartbeat
                                                        // datanodes:hour_time*3600

    int killConnection(long id); // kill @@connection id

    boolean reloadConfig();// reload @@config

    boolean rollbackConfig();// rollback @@config

    boolean checkConnection(); // use show @@version to check cobar connection

}
