package com.taobao.tddl.monitor.logger;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.utils.version.Version;
import com.taobao.tddl.monitor.logger.log4j.DynamicLog4jAdapterLogger;
import com.taobao.tddl.monitor.logger.log4j.DynamicLog4jLogger;
import com.taobao.tddl.monitor.logger.logback.DynamicLogback918Logger;
import com.taobao.tddl.monitor.logger.logback.DynamicLogbackLogger;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class LoggerInit {

    public static final Logger TDDL_DYNAMIC_CONFIG = LoggerFactory.getLogger("TDDL_DYNAMIC_CONFIG");
    // add by changyuan.lh, db 应用连接数, 阻塞时间, 超时数
    public static final Logger TDDL_STAT_LOG       = LoggerFactory.getLogger("TDDL_STAT_LOG");
    // tddl rule相关日志，需要独立出来，rule会被多个地方共享
    public static final Logger logger              = LoggerFactory.getLogger("com.taobao.tddl");

    private static boolean     canUseEncoder       = false;
    static {
        try {
            // logback从0.9.19开始采用encoder
            // http://logback.qos.ch/manual/encoders.html
            Class.forName("ch.qos.logback.classic.encoder.PatternLayoutEncoder");
            canUseEncoder = true;
        } catch (ClassNotFoundException e) {
            canUseEncoder = false;
        }

        initTddlLog();
    }

    static public void initTddlLog() {
        DynamicLogger dynamic = buildDynamic();

        if (dynamic != null) {
            dynamic.init();
        }

        TDDL_DYNAMIC_CONFIG.info(Version.getBuildVersion());
    }

    private synchronized static DynamicLogger buildDynamic() {
        DynamicLogger dynamic = null;
        String LOGBACK = "logback";
        String LOG4J_Adapter = "Log4jLoggerAdapter";
        String LOG4J = "log4j";

        // slf4j只是一个代理工程，需要判断一下具体的实现类
        if (checkLogger(logger, LOGBACK)) {
            if (canUseEncoder) {
                dynamic = new DynamicLogbackLogger();
            } else {
                dynamic = new DynamicLogback918Logger();
            }
        } else if (checkLogger(logger, LOG4J_Adapter)) {
            dynamic = new DynamicLog4jAdapterLogger();
        } else if (checkLogger(logger, LOG4J)) {
            dynamic = new DynamicLog4jLogger();
        } else {
            logger.warn("logger is not a log4j/logback instance, dynamic logger is disabled");
        }
        return dynamic;
    }

    private static boolean checkLogger(Logger logger, String name) {
        return StringUtils.contains(logger.getDelegate().getClass().getName(), name);
    }
}
