package com.alibaba.cobar.manager.web;

import java.io.InputStream;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class LogbackConfiguratorListener implements ServletContextListener {

    private static final Logger logger          = LoggerFactory.getLogger(LogbackConfiguratorListener.class);

    private static final String CONFIG_LOCATION = "logbackConfigLocation";

    // static {
    // System.setProperty("org.apache.commons.logging.Log",
    // "org.apache.commons.logging.impl.SLF4JLog");
    // }

    @Override
    public void contextInitialized(ServletContextEvent event) {
        // 从web.xml中加载指定文件名的日志配置文件
        String logbackConfigLocation = event.getServletContext().getInitParameter(CONFIG_LOCATION);
        InputStream fn = event.getClass().getClassLoader().getResourceAsStream(logbackConfigLocation);
        if (fn == null) {
            return;
        }
        try {
            LoggerContext loggerContext = (LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory();
            loggerContext.reset();
            JoranConfigurator joranConfigurator = new JoranConfigurator();
            joranConfigurator.setContext(loggerContext);
            joranConfigurator.doConfigure(fn);
            logger.debug("loaded slf4j configure file from " + fn);
        } catch (JoranException e) {
            logger.error("can loading slf4j configure file from " + fn, e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
    }
}
