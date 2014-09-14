package com.taobao.tddl.matrix.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.TddlConstants;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.common.model.App;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.plugin.PreSqlPlugin;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.thread.CallerRunExecutorService;
import com.taobao.tddl.common.utils.thread.NamedThreadFactory;
import com.taobao.tddl.config.ConfigDataMode;
import com.taobao.tddl.executor.MatrixExecutor;
import com.taobao.tddl.matrix.config.MatrixConfigHolder;
import com.taobao.tddl.monitor.logger.LoggerInit;
import com.taobao.tddl.monitor.unit.RouterUnitsHelper;
import com.taobao.tddl.statistics.SQLRecorder;

/**
 * matrix的jdbc datasource实现
 * 
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:14
 * @since 5.0.0
 */
public class TDataSource extends AbstractLifecycle implements DataSource {

    private String                               ruleFilePath          = null;
    private boolean                              sharding              = true;                      // 是否不做sharding,如果为false跳过rule初始化
    private String                               topologyFile          = null;
    private String                               schemaFile            = null;
    private String                               appName               = null;
    private String                               unitName              = null;
    private boolean                              dynamicRule           = true;                      // 是否使用动态规则
    private MatrixExecutor                       executor              = null;
    private Map<String, Object>                  connectionProperties  = new HashMap(2);
    private MatrixConfigHolder                   configHolder;
    private List<App>                            subApps               = new ArrayList();
    /**
     * 用于并行查询的线程池
     */
    private ExecutorService                      globalExecutorService = null;
    private boolean                              shareGlobalExecutor   = false;
    private LinkedBlockingQueue<ExecutorService> executorServiceQueue  = null;
    private String                               sequenceFile;
    private List<PreSqlPlugin>                   preSqlPlugins;
    private String                               configMode;
    // 写入模式，取值: center/unit (如果是center，并且当前是unit环境，则不启动tddl)
    private String                               writeMode             = null;
    private SQLRecorder                          recorder              = new SQLRecorder(100, true);
    private Long                                 maxActive;
    private AtomicLong                           activeCount           = new AtomicLong(0);

    @Override
    public void doInit() throws TddlException {
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("TDataSource start init");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("appName is: " + appName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("unitName is: " + unitName);
        if (isIgnoreInit()) {
            // 主机房模式,当前又不在主机房,不启动
            LoggerInit.TDDL_DYNAMIC_CONFIG.info("start by in unit , so ignore");
            return;
        }
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("schemaFile is: " + this.schemaFile);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("ruleFile is: " + this.ruleFilePath);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("topologyFile is: " + this.topologyFile);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("subApps is: " + this.subApps);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("writeMode is: " + this.writeMode);
        if (StringUtils.isNotEmpty(this.configMode)) {
            LoggerInit.TDDL_DYNAMIC_CONFIG.info("configMode is: " + this.configMode);
            System.setProperty(ConfigDataMode.CONFIG_MODE, this.configMode);
        }

        this.executor = new MatrixExecutor();
        executor.init();

        MatrixConfigHolder configHolder = new MatrixConfigHolder();
        configHolder.setAppName(appName);
        configHolder.setSubApps(subApps);
        configHolder.setUnitName(unitName);
        configHolder.setTopologyFilePath(this.topologyFile);
        configHolder.setSchemaFilePath(this.schemaFile);
        configHolder.setSequenceFile(this.sequenceFile);
        configHolder.setRuleFilePath(this.ruleFilePath);
        configHolder.setConnectionProperties(this.connectionProperties);
        configHolder.setDynamicRule(dynamicRule);
        configHolder.setSharding(this.sharding);
        configHolder.init();

        this.configHolder = configHolder;

        /**
         * 如果不为每个连接都初始化，则为整个ds初始化一个线程池
         */
        boolean everyConnectionPool = GeneralUtil.getExtraCmdBoolean(this.getConnectionProperties(),
            ConnectionProperties.INIT_CONCURRENT_POOL_EVERY_CONNECTION,
            true);
        maxActive = GeneralUtil.getExtraCmdLong(this.getConnectionProperties(),
            ConnectionProperties.MAX_CONCURRENT_THREAD_SIZE,
            TddlConstants.MAX_CONCURRENT_THREAD_SIZE);
        if (everyConnectionPool) {
            executorServiceQueue = new LinkedBlockingQueue<ExecutorService>();
        } else if (globalExecutorService == null) {
            // 全局共享线程池
            Object poolSizeObj = GeneralUtil.getExtraCmdString(this.getConnectionProperties(),
                ConnectionProperties.CONCURRENT_THREAD_SIZE);

            if (poolSizeObj == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG,
                    "如果线程池为整个datasource共用，请使用CONCURRENT_THREAD_SIZE指定线程池大小");
            }

            int poolSize = Integer.valueOf(poolSizeObj.toString());
            // 默认queue队列为poolSize的两倍，超过queue大小后使用当前线程
            globalExecutorService = createThreadPool(poolSize, false);
        }
    }

    @Override
    public TConnection getConnection() throws SQLException {
        try {
            if (isIgnoreInit()) {
                throw new TddlException(ErrorCode.ERR_CONFIG, "start by in unit , so ignore");
            }

            return new TConnection(this);
        } catch (Exception e) {
            throw new SQLException(e);

        }
    }

    public ExecutorService borrowExecutorService() {
        if (globalExecutorService != null) {
            return globalExecutorService;
        } else {
            ExecutorService executor = executorServiceQueue.poll();
            if (executor == null) {
                Object poolSizeObj = GeneralUtil.getExtraCmdString(this.getConnectionProperties(),
                    ConnectionProperties.CONCURRENT_THREAD_SIZE);
                int poolSize = 0;
                if (poolSizeObj != null) {
                    poolSize = Integer.valueOf(poolSizeObj.toString());
                } else {
                    poolSize = TddlConstants.DEFAULT_CONCURRENT_THREAD_SIZE;
                }

                if (activeCount.addAndGet(poolSize) <= maxActive) {
                    executor = createThreadPool(poolSize, false);
                } else {
                    executor = createThreadPool(poolSize, true);
                }
            }

            if (executor.isShutdown()) {
                return borrowExecutorService();
            } else {
                return executor;
            }
        }
    }

    public void releaseExecutorService(ExecutorService executor) {
        if (executor != null && executor != globalExecutorService) {
            executorServiceQueue.offer(executor);// 放回队列中
        }
    }

    private ExecutorService createThreadPool(int poolSize, boolean caller) {
        if (caller) {
            return new CallerRunExecutorService();
        } else {
            return new ThreadPoolExecutor(poolSize,
                poolSize,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue(poolSize * 2),
                new NamedThreadFactory("tddl_concurrent_query_executor", true),
                new ThreadPoolExecutor.CallerRunsPolicy());
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return this.getConnection();
    }

    @Override
    public void doDestroy() throws TddlException {
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("TDataSource stop");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("appName is: " + appName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("unitName is: " + unitName);
        if (isIgnoreInit()) {
            return;
        }

        if (!shareGlobalExecutor && globalExecutorService != null) {
            globalExecutorService.shutdownNow();
        }

        if (executorServiceQueue != null) {
            for (ExecutorService executor : executorServiceQueue) {
                executor.shutdownNow();
            }

            executorServiceQueue.clear();
        }

        if (configHolder != null) {
            configHolder.destroy();
        }

        if (executor != null && executor.isInited()) {
            executor.destroy();
        }
    }

    /**
     * 当前为非中心站点，并且当前dataSource配置为主站点，则忽略本次启动
     */
    private boolean isIgnoreInit() {
        return StringUtils.equalsIgnoreCase(writeMode, "center") && !RouterUnitsHelper.isCenterUnit();
    }

    public String getRuleFile() {
        return ruleFilePath;
    }

    public void setRuleFile(String ruleFilePath) {
        this.ruleFilePath = ruleFilePath;
        if (this.ruleFilePath != null) {
            setSharding(true);
        }
    }

    public String getTopologyFile() {
        return topologyFile;
    }

    public void setTopologyFile(String topologyFile) {
        this.topologyFile = topologyFile;
    }

    public String getSchemaFile() {
        return schemaFile;
    }

    public void setSchemaFile(String schemaFile) {
        this.schemaFile = schemaFile;
    }

    public MatrixExecutor getExecutor() {
        return this.executor;
    }

    public Map<String, Object> getConnectionProperties() {
        return this.connectionProperties;
    }

    public void setConnectionProperties(Map<String, Object> cp) {
        this.connectionProperties = cp;
    }

    public void putConnectionProperties(String name, Object value) {
        if (this.connectionProperties == null) {
            this.connectionProperties = new HashMap<String, Object>();
        }

        this.connectionProperties.put(name, value);
    }

    public void setAppName(String appName) {

        // if (appName != null) {
        // appName = appName.trim();
        // }
        this.appName = appName;

    }

    public MatrixConfigHolder getConfigHolder() {
        return this.configHolder;
    }

    public void setDynamicRule(boolean dynamicRule) {
        this.dynamicRule = dynamicRule;
        if (this.dynamicRule) {
            setSharding(true);
        }
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public boolean isSharding() {
        return sharding;
    }

    public void setSharding(boolean sharding) {
        this.sharding = sharding;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new UnsupportedOperationException("getLogWriter");
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new UnsupportedOperationException("getLoginTimeout");
    }

    @Override
    public void setLogWriter(PrintWriter arg0) throws SQLException {
        throw new UnsupportedOperationException("setLogWriter");

    }

    @Override
    public void setLoginTimeout(int arg0) throws SQLException {
        throw new UnsupportedOperationException("setLoginTimeout");

    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.getClass().isAssignableFrom(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public void addSubApp(String appName) {
        App app = new App();
        app.setAppName(appName);
        this.subApps.add(app);
    }

    public void addSubApp(App app) {
        this.subApps.add(app);
    }

    public void setSubApps(List<App> subApps) {
        this.subApps = subApps;
    }

    public String getSequenceFile() {
        return sequenceFile;
    }

    public void setSequenceFile(String sequenceFile) {
        this.sequenceFile = sequenceFile;
    }

    public void setPreSqlPluginList(List<PreSqlPlugin> preSqlPlugins) {
        this.preSqlPlugins = preSqlPlugins;
    }

    public List<PreSqlPlugin> getPreSqlPluginList() {
        return this.preSqlPlugins;
    }

    public String getConfigMode() {
        return configMode;
    }

    public void setConfigMode(String configMode) {
        this.configMode = configMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setGlobalExecutorService(ExecutorService globalExecutorService) {
        this.globalExecutorService = globalExecutorService;
        this.shareGlobalExecutor = true;
    }

    public SQLRecorder getRecorder() {
        return this.recorder;

    }

    public void setRecorder(SQLRecorder recorder) {
        this.recorder = recorder;
    }

}
