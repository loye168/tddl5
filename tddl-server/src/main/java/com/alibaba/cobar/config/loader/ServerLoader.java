package com.alibaba.cobar.config.loader;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.Isolations;
import com.alibaba.cobar.config.ConfigException;
import com.alibaba.cobar.config.QuarantineConfig;
import com.alibaba.cobar.config.SystemConfig;
import com.alibaba.cobar.server.util.StringUtil;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;

public final class ServerLoader extends AbstractLifecycle implements Lifecycle {

    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    private final SystemConfig  system;

    public ServerLoader(){
        this.system = new SystemConfig();
    }

    @Override
    protected void doInit() throws TddlException {
        this.load();
    }

    @Override
    protected void doDestroy() throws TddlException {
        super.doDestroy();
    }

    private void load() {
        String conf = System.getProperty("server.conf", "classpath:server.properties");
        Properties serverProps = new Properties();
        try {
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                serverProps.load(CobarServer.class.getClassLoader().getResourceAsStream(conf));
            } else {
                serverProps.load(new FileInputStream(conf));
            }
        } catch (IOException e) {
            throw new TddlNestableRuntimeException(e);
        }

        configSystem(serverProps);

        QuarantineConfig.getInstance().resetTrustedIps(system.getTrustedIps());
    }

    private void configSystem(Properties serverProps) {
        if (serverProps == null) {
            throw new ConfigException("server properties is null!");
        }

        String serverPort = serverProps.getProperty("serverPort");
        if (!StringUtil.isEmpty(serverPort)) {
            this.system.setServerPort(Integer.parseInt(serverPort));
        }

        String managerPort = serverProps.getProperty("managerPort");
        if (!StringUtil.isEmpty(managerPort)) {
            this.system.setManagerPort(Integer.parseInt(managerPort));
        }

        String charset = serverProps.getProperty("charset");
        if (!StringUtil.isEmpty(charset)) {
            this.system.setCharset(charset);
        }

        String processorCount = serverProps.getProperty("processors");
        if (!StringUtil.isEmpty(processorCount)) {
            this.system.setProcessors(Integer.parseInt(processorCount));
        }

        String processorHandlerCount = serverProps.getProperty("processorHandler");
        if (!StringUtil.isEmpty(processorHandlerCount)) {
            this.system.setProcessorHandler(Integer.parseInt(processorHandlerCount));
        }

        String serverExecutorCount = serverProps.getProperty("serverExecutor");
        if (!StringUtil.isEmpty(serverExecutorCount)) {
            this.system.setServerExecutor(Integer.parseInt(serverExecutorCount));
        }

        String processorKillExecutorCount = serverProps.getProperty("processorKillExecutor");
        if (!StringUtil.isEmpty(processorKillExecutorCount)) {
            this.system.setProcessorKillExecutor(Integer.parseInt(processorKillExecutorCount));
        }

        String initExecutorCount = serverProps.getProperty("initExecutor");
        if (!StringUtil.isEmpty(initExecutorCount)) {
            this.system.setInitExecutor(Integer.parseInt(initExecutorCount));
        }

        String timerExecutorCount = serverProps.getProperty("timerExecutor");
        if (!StringUtil.isEmpty(timerExecutorCount)) {
            this.system.setTimerExecutor(Integer.parseInt(timerExecutorCount));
        }

        String managerExecutorCount = serverProps.getProperty("managerExecutor");
        if (!StringUtil.isEmpty(managerExecutorCount)) {
            this.system.setManagerExecutor(Integer.parseInt(managerExecutorCount));
        }

        String idleTimeoutCount = serverProps.getProperty("idleTimeout");
        if (!StringUtil.isEmpty(idleTimeoutCount)) {
            this.system.setIdleTimeout(Integer.parseInt(idleTimeoutCount));
        }

        String txIsolation = serverProps.getProperty("txIsolation");
        if (!StringUtil.isEmpty(txIsolation)) {
            this.system.setTxIsolation(Isolations.valuesOf(txIsolation).getCode());
        }

        String parserCommentVersion = serverProps.getProperty("parserCommentVersion");
        if (!StringUtil.isEmpty(parserCommentVersion)) {
            this.system.setParserCommentVersion(Integer.valueOf(parserCommentVersion));
        }

        String sqlRecordCount = serverProps.getProperty("sqlRecordCount");
        if (!StringUtil.isEmpty(sqlRecordCount)) {
            this.system.setParserCommentVersion(Integer.valueOf(sqlRecordCount));
        }

        String timeUpdatePeriod = serverProps.getProperty("timeUpdatePeriod");
        if (!StringUtil.isEmpty(timeUpdatePeriod)) {
            this.system.setTimeUpdatePeriod(Long.parseLong(timeUpdatePeriod));
        }

        // 集群名称
        String clusterName = serverProps.getProperty("cluster");
        if (!StringUtil.isEmpty(clusterName)) {
            this.system.setClusterName(clusterName);
        }

        // 信任host ip列表
        String trustedIps = serverProps.getProperty("trustedips");
        if (!StringUtil.isEmpty(trustedIps)) {
            this.system.setTrustedIps(trustedIps);
        }
    }

    public SystemConfig getSystem() {
        return system;
    }

}
