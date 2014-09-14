package com.taobao.tddl.monitor.logger.logback;

import java.io.File;
import java.util.Map;

import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;

import com.taobao.tddl.monitor.logger.DynamicLogger;
import com.taobao.tddl.monitor.logger.LoggerInit;

public class DynamicLogback918Logger extends DynamicLogger {

    protected static LoggerContext loggerContext;

    public DynamicLogback918Logger(){
        buildLoggerContext(null);
    }

    public void init() {
        Appender statAppender = buildAppender("TDDL_Stat_Appender",
            "tddl-stat.log",
            "01 %d{yyyy-MM-dd HH:mm:ss.SSS} %p [%-5t:%c{2}] [] [] [] %m%n");
        Appender dynamicAppender = buildAppender("TDDL_Dynamic_Config",
            "tddl-dynamic.log",
            "01 %d{yyyy-MM-dd HH:mm:ss.SSS} %p [%-5t:%c{2}] [] [] [] %m%n");

        ch.qos.logback.classic.Logger logger = (Logger) LoggerInit.TDDL_STAT_LOG.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(statAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.TDDL_DYNAMIC_CONFIG.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(dynamicAppender);
        logger.setLevel(Level.INFO);
    }

    protected Appender buildAppender(String name, String fileName, String pattern) {
        RollingFileAppender appender = new RollingFileAppender();
        appender.setContext(loggerContext);
        appender.setName(name);
        appender.setAppend(true);
        appender.setFile(new File(getLogPath(), fileName).getAbsolutePath());

        TimeBasedRollingPolicy rolling = new TimeBasedRollingPolicy();
        rolling.setParent(appender);
        rolling.setFileNamePattern(new File(getLogPath(), fileName).getAbsolutePath() + ".%d{yyyy-MM-dd}");
        rolling.setContext(loggerContext);
        rolling.start();
        appender.setRollingPolicy(rolling);

        PatternLayout layout = new PatternLayout();
        layout.setPattern(pattern);
        layout.setContext(loggerContext);
        layout.start();
        appender.setLayout(layout);
        // 启动
        appender.start();
        return appender;
    }

    protected Appender buildDailyMaxRollingAppender(String name, String fileName, String pattern, int maxBackupIndex) {
        RollingFileAppender appender = new RollingFileAppender();
        appender.setContext(loggerContext);
        appender.setName(name);
        appender.setAppend(true);
        appender.setFile(new File(getLogPath(), fileName).getAbsolutePath());

        TimeBasedRollingPolicy rolling = new TimeBasedRollingPolicy();
        rolling.setContext(loggerContext);
        rolling.setFileNamePattern(new File(getLogPath(), fileName).getAbsolutePath() + ".%d{yyyy-MM-dd-HH}");
        rolling.setMaxHistory(maxBackupIndex);
        rolling.setParent(appender);
        rolling.start();
        appender.setRollingPolicy(rolling);

        PatternLayout layout = new PatternLayout();
        layout.setContext(loggerContext);
        layout.setPattern(pattern);
        layout.start();
        appender.setLayout(layout);
        // 启动
        appender.start();
        return appender;
    }

    public static LoggerContext buildLoggerContext(Map<String, String> props) {
        if (loggerContext == null) {
            ILoggerFactory lcObject = LoggerFactory.getILoggerFactory();

            if (!(lcObject instanceof LoggerContext)) {
                throw new LogbackException("Expected LOGBACK binding with SLF4J, but another log system has taken the place: "
                                           + lcObject.getClass().getSimpleName());
            }

            loggerContext = (LoggerContext) lcObject;
            if (props != null) {
                for (Map.Entry<String, String> entry : props.entrySet()) {
                    loggerContext.putProperty(entry.getKey(), entry.getValue());
                }
            }
        }

        return loggerContext;
    }

}
