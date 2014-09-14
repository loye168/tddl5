package com.taobao.tddl.monitor;

import com.taobao.tddl.monitor.logger.LoggerInit;
import com.taobao.tddl.monitor.stat.AbstractStatLogWriter.LogCounter;

public class Monitor extends MonitorConfig {

    public static final String KEY1                = "TDDL";

    /**
     * TDDL 数据库（分桶）连接数
     */
    public static final String KEY3_CONN_NUMBER    = "CONN_NUM";
    /**
     * TDDL 数据库（分桶）连接阻塞时间
     */
    public static final String KEY3_CONN_BLOCKING  = "CONN_BLOCKING";

    public static final String KEY2_TDDL_RULE      = "TDDL_RULE";
    public static final String KEY2_TDDL_PARSE     = "TDDL_PARSE";
    public static final String KEY2_TDDL_OPTIMIZER = "TDDL_OPTIMIZER";
    public static final String KEY2_TDDL_EXECUTE   = "TDDL_EXECUTE";

    public static final String Key3Success         = "success";
    public static final String Key3Fail            = "fail";
    public static final String Key3FutureDone      = "futureDone";
    public static final String Key3FutureGet       = "futureGet";

    static {
        LoggerInit.initTddlLog();
    }

    public static long monitorAndRenewTime(String key1, String key2, String key3, long count, long time) {
        bufferedStatLogWriter.stat(key1, key2, key3, count, System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
        return time;
    }

    public static long monitorAndRenewTime(String key1, String key2, String key3, long time) {
        bufferedStatLogWriter.stat(key1, key2, key3, System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
        return time;
    }

    /**
     * 获得一个统计对象, 不用可以直接抛弃 <br/>
     * 举个例子：datasourceKey, "-", Monitor.KEY3_CONN_NUMBER
     * 
     * @param obj1
     * @param obj2
     * @param obj3
     * @return
     */
    public static LogCounter connStat(String obj1, String obj2, String obj3) {
        Object[] objs = new Object[] { obj1, obj2, obj3 };
        return connRefStatLogWriter.getCounter(objs, objs);
    }

    // ====================== helper method ==============================

    public static synchronized void addSnapshotValuesCallbask(SnapshotValuesOutputCallBack callbackList) {
        statMonitor.addSnapshotValuesCallbask(callbackList);
    }

    public static synchronized void removeSnapshotValuesCallback(SnapshotValuesOutputCallBack callbackList) {
        statMonitor.removeSnapshotValuesCallback(callbackList);
    }

}
