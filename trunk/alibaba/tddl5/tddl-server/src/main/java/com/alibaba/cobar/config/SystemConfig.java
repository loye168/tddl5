package com.alibaba.cobar.config;

/**
 * 系统基础配置项
 * 
 * @author xianmao.hexm 2011-1-11 下午02:14:04
 */
public final class SystemConfig {

    private static final int    DEFAULT_PORT                   = 8507;
    private static final int    DEFAULT_MANAGER_PORT           = 9507;
    private static final String DEFAULT_CHARSET                = "UTF-8";
    private static final int    DEFAULT_PROCESSORS             = Runtime.getRuntime().availableProcessors();
    private static final long   DEFAULT_IDLE_TIMEOUT           = 8 * 3600 * 1000L;
    private static final long   DEFAULT_PROCESSOR_CHECK_PERIOD = 15 * 1000L;
    private static final int    DEFAULT_PARSER_COMMENT_VERSION = 50148;
    private static final int    DEFAULT_SQL_RECORD_COUNT       = 10;
    private static final long   DEFAULT_TIME_UPDATE_PERIOD     = 20L;

    private int                 serverPort;
    private int                 managerPort;
    private String              charset;
    private int                 processors;
    private int                 processorHandler;
    private int                 processorKillExecutor;
    private int                 initExecutor;
    private int                 timerExecutor;
    private int                 serverExecutor;
    private int                 managerExecutor;
    private long                idleTimeout;
    private long                processorCheckPeriod;
    // 代表不设置，使用数据库默认
    private int                 txIsolation                    = -1;
    private int                 parserCommentVersion;
    private int                 sqlRecordCount;
    private long                timeUpdatePeriod;
    // corona集群名称
    private String              clusterName;
    // 信任的ip子网列表
    private String              trustedIps;

    public SystemConfig(){
        this.serverPort = DEFAULT_PORT;
        this.managerPort = DEFAULT_MANAGER_PORT;
        this.charset = DEFAULT_CHARSET;
        this.processors = DEFAULT_PROCESSORS;
        this.processorHandler = DEFAULT_PROCESSORS;
        this.serverExecutor = DEFAULT_PROCESSORS;
        this.processorKillExecutor = DEFAULT_PROCESSORS;
        this.managerExecutor = DEFAULT_PROCESSORS;
        this.timerExecutor = DEFAULT_PROCESSORS;
        this.initExecutor = DEFAULT_PROCESSORS;
        this.idleTimeout = DEFAULT_IDLE_TIMEOUT;
        this.processorCheckPeriod = DEFAULT_PROCESSOR_CHECK_PERIOD;
        // this.txIsolation = Isolations.REPEATED_READ.getCode();
        this.parserCommentVersion = DEFAULT_PARSER_COMMENT_VERSION;
        this.sqlRecordCount = DEFAULT_SQL_RECORD_COUNT;
        this.timeUpdatePeriod = DEFAULT_TIME_UPDATE_PERIOD;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    public int getManagerPort() {
        return managerPort;
    }

    public void setManagerPort(int managerPort) {
        this.managerPort = managerPort;
    }

    public int getProcessors() {
        return processors;
    }

    public void setProcessors(int processors) {
        this.processors = processors;
    }

    public int getProcessorHandler() {
        return processorHandler;
    }

    public void setProcessorHandler(int processorExecutor) {
        this.processorHandler = processorExecutor;
    }

    public int getServerExecutor() {
        return serverExecutor;
    }

    public void setServerExecutor(int serverExecutor) {
        this.serverExecutor = serverExecutor;
    }

    public int getProcessorKillExecutor() {
        return processorKillExecutor;
    }

    public void setProcessorKillExecutor(int processorKillExecutor) {
        this.processorKillExecutor = processorKillExecutor;
    }

    public int getManagerExecutor() {
        return managerExecutor;
    }

    public void setManagerExecutor(int managerExecutor) {
        this.managerExecutor = managerExecutor;
    }

    public int getTimerExecutor() {
        return timerExecutor;
    }

    public void setTimerExecutor(int timerExecutor) {
        this.timerExecutor = timerExecutor;
    }

    public int getInitExecutor() {
        return initExecutor;
    }

    public void setInitExecutor(int initExecutor) {
        this.initExecutor = initExecutor;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public long getProcessorCheckPeriod() {
        return processorCheckPeriod;
    }

    public void setProcessorCheckPeriod(long processorCheckPeriod) {
        this.processorCheckPeriod = processorCheckPeriod;
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
    }

    public int getParserCommentVersion() {
        return parserCommentVersion;
    }

    public void setParserCommentVersion(int parserCommentVersion) {
        this.parserCommentVersion = parserCommentVersion;
    }

    public int getSqlRecordCount() {
        return sqlRecordCount;
    }

    public void setSqlRecordCount(int sqlRecordCount) {
        this.sqlRecordCount = sqlRecordCount;
    }

    public long getTimeUpdatePeriod() {
        return timeUpdatePeriod;
    }

    public void setTimeUpdatePeriod(long timeUpdatePeriod) {
        this.timeUpdatePeriod = timeUpdatePeriod;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getTrustedIps() {
        return trustedIps;
    }

    public void setTrustedIps(String trustedIps) {
        this.trustedIps = trustedIps;
    }
}
