package com.taobao.tddl.monitor.logger.log4j;

import java.io.File;

import org.apache.log4j.Appender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;

import com.taobao.tddl.monitor.logger.DynamicLogger;
import com.taobao.tddl.monitor.logger.LoggerInit;

import com.taobao.tddl.common.utils.logger.Logger;

public class DynamicLog4jLogger extends DynamicLogger {

    public void init() {
        Appender statAppender = buildDailyMaxRollingAppender("TDDL_Stat_Appender",
            "tddl-stat.log",
            "01 %d{yyyy-MM-dd HH:mm:ss.SSS} %p [%-5t:%c{2}] [] [] [] %m%n",
            6);
        Appender dynamicAppender = buildAppender("TDDL_Dynamic_Config",
            "tddl-dynamic.log",
            "01 %d{yyyy-MM-dd HH:mm:ss.SSS} %p [%-5t:%c{2}] [] [] [] %m%n");

        org.apache.log4j.Logger logger = getLog4jLogger(LoggerInit.TDDL_STAT_LOG);
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(statAppender);
        logger.setLevel(Level.INFO);

        logger = getLog4jLogger(LoggerInit.TDDL_DYNAMIC_CONFIG);
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(dynamicAppender);
        logger.setLevel(Level.INFO);
    }

    protected org.apache.log4j.Logger getLog4jLogger(Logger logger) {
        org.apache.log4j.Logger log4j = (org.apache.log4j.Logger) logger.getDelegate();
        return log4j;
    }

    private Appender buildAppender(String name, String fileName, String pattern) {
        DailyRollingFileAppender appender = new DailyRollingFileAppender();
        appender.setName(name);
        appender.setAppend(true);
        appender.setEncoding(getEncoding());
        appender.setLayout(new PatternLayout(pattern));
        appender.setFile(new File(getLogPath(), fileName).getAbsolutePath());
        appender.activateOptions();// 很重要，否则原有日志内容会被清空
        return appender;
    }

    private Appender buildDailyMaxRollingAppender(String name, String fileName, String pattern, int maxBackupIndex) {
        DailyMaxRollingFileAppender appender = new DailyMaxRollingFileAppender();
        appender.setName(name);
        appender.setAppend(true);
        appender.setEncoding(getEncoding());
        appender.setLayout(new PatternLayout(pattern));
        appender.setDatePattern("'.'yyyy-MM-dd-HH");
        appender.setMaxBackupIndex(maxBackupIndex);
        appender.setFile(new File(getLogPath(), fileName).getAbsolutePath());
        appender.activateOptions();// 很重要，否则原有日志内容会被清空
        return appender;
    }

}
