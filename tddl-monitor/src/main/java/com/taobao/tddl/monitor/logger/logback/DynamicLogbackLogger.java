package com.taobao.tddl.monitor.logger.logback;

import java.io.File;
import java.nio.charset.Charset;

import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;

public class DynamicLogbackLogger extends DynamicLogback918Logger {

    protected Appender buildAppender(String name, String fileName, String pattern) {
        RollingFileAppender appender = new RollingFileAppender();
        appender.setContext(loggerContext);
        appender.setName(name);
        appender.setAppend(true);
        appender.setFile(new File(getLogPath(), fileName).getAbsolutePath());

        TimeBasedRollingPolicy rolling = new TimeBasedRollingPolicy();
        rolling.setContext(loggerContext);
        rolling.setParent(appender);
        rolling.setFileNamePattern(new File(getLogPath(), fileName).getAbsolutePath() + ".%d{yyyy-MM-dd}");
        rolling.start();
        appender.setRollingPolicy(rolling);

        PatternLayoutEncoder layout = new PatternLayoutEncoder();
        layout.setContext(loggerContext);
        layout.setPattern(pattern);
        layout.setCharset(Charset.forName(getEncoding()));
        layout.start();
        appender.setEncoder(layout);
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

        PatternLayoutEncoder layout = new PatternLayoutEncoder();
        layout.setContext(loggerContext);
        layout.setPattern(pattern);
        layout.setCharset(Charset.forName(getEncoding()));
        layout.start();
        appender.setEncoder(layout);
        // 启动
        appender.start();
        return appender;
    }

}
