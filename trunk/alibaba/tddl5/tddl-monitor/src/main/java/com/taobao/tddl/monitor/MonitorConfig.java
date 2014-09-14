package com.taobao.tddl.monitor;

import com.taobao.tddl.monitor.logger.LoggerInit;
import com.taobao.tddl.monitor.stat.BufferedLogWriter;
import com.taobao.tddl.monitor.stat.LoggerLogWriter;
import com.taobao.tddl.monitor.stat.MinMaxAvgLogWriter;
import com.taobao.tddl.monitor.stat.SoftRefLogWriter;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 维护Monitor需要的参数
 * 
 * @author jianghang 2013-10-30 下午5:49:16
 * @since 5.0.0
 */
public class MonitorConfig {

    static final Logger                   logger                = LoggerFactory.getLogger(Monitor.class);          // 使用monitor.class，兼容
    public static volatile int            left                  = 0;                                               // 从左起保留多少个字符
    public static volatile int            right                 = 0;                                               // 从右起保留多少个字符

    public static final StatMonitor       statMonitor           = StatMonitor.getInstance();

    /** changyuan.lh: TDDL 统计日志 */
    /* 记录行复制日志与 SQL 解析日志, Key 的量与 SQL 数量相同 */
    public static final BufferedLogWriter bufferedStatLogWriter = new BufferedLogWriter(1024,
                                                                    4096,
                                                                    new LoggerLogWriter(LoggerInit.TDDL_STAT_LOG));
    /* 记录 Atom 连接池以及业务分桶的连接申请记录, Key 量最大是物理库 x 业务分桶数量 */
    public static final SoftRefLogWriter  connRefStatLogWriter  = new SoftRefLogWriter(false,
                                                                    new MinMaxAvgLogWriter(", ",
                                                                        LoggerInit.TDDL_STAT_LOG));

}
