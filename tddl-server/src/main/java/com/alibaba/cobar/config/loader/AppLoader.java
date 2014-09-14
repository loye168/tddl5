package com.alibaba.cobar.config.loader;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.config.QuarantineConfig;
import com.alibaba.cobar.config.SchemaConfig;
import com.alibaba.cobar.config.UserConfig;
import com.alibaba.cobar.server.util.StringUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.ConfigDataHandlerCity;
import com.taobao.tddl.matrix.jdbc.TDataSource;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 加载一个appname对应的资源,比如用户密码/tddl数据源等
 * 
 * @author jianghang 2014-5-28 下午5:09:41
 * @since 5.1.0
 */
public final class AppLoader extends AbstractLifecycle implements Lifecycle {

    private static final Logger              logger            = LoggerFactory.getLogger(AppLoader.class);
    private static final String              DATAID_PREFIX     = "com.taobao.corona.";
    private static final String              DATAID_SUFFIX     = ".passwd";
    private static final String              QUARANTINE_SURFIX = ".quarantine";
    private ConfigDataHandlerFactory         cdhf              = ConfigDataHandlerCity.getFactory(null, null);
    private Cache<String, ConfigDataHandler> cdhs              = CacheBuilder.newBuilder().build();
    private String                           cluster;
    private Map<String/* app */, UserConfig> users;
    private Map<String, SchemaConfig>        schemas;
    private QuarantineConfig                 quarantine;

    public AppLoader(String cluster){
        this.cluster = cluster;
    }

    @Override
    protected void doInit() throws TddlException {
        this.users = new HashMap<String, UserConfig>();
        this.schemas = new HashMap<String, SchemaConfig>();
        this.quarantine = QuarantineConfig.getInstance();
    }

    /**
     * 装载app
     */
    public void loadApp(final String app) {
        try {
            logger.info("start loading app:" + app);
            this.loadUser(app);
            this.loadSchema(app);
            this.loadQuarantine(app);
            logger.info("finish loading app:" + app);
        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    /**
     * Do not add cluster to keep compatible with old tddl
     * 
     * @param app
     * @throws ExecutionException
     * @throws TddlException
     */
    private void loadQuarantine(String app) throws ExecutionException, TddlException {
        String dataId = DATAID_PREFIX + app + QUARANTINE_SURFIX;
        ConfigDataHandler cdh = getQuarantineDataHandler(app, dataId);
        String config = cdh.getData();
        this.parseQuarantineConfig(dataId, app, config);
    }

    /**
     * 卸载app
     */
    public void unLoadApp(final String app) {
        try {
            logger.info("start unLoading app:" + app);
            this.unLoadUser(app);
            this.unLoadSchema(app);
            logger.info("finish unLoading app:" + app);
        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    private synchronized void loadUser(final String app) throws ExecutionException, TddlException {
        String dataId = DATAID_PREFIX + this.cluster + "." + app + DATAID_SUFFIX;
        ConfigDataHandler cdh = getConfigDataHandler(app, dataId);
        String config = cdh.getData();
        if (StringUtil.isEmpty(config)) {
            cdhs.invalidate(dataId);
            cdh.destroy();

            // 重新拿一下新的dataId
            String newDataId = DATAID_PREFIX + app + DATAID_SUFFIX;
            cdh = getConfigDataHandler(app, newDataId);
            config = cdh.getData();
        }

        this.parseConfig(dataId, app, config);
    }

    private synchronized void unLoadUser(final String app) throws TddlException {
        String dataId = DATAID_PREFIX + this.cluster + "." + app + DATAID_SUFFIX;
        ConfigDataHandler cdh = cdhs.getIfPresent(dataId);
        if (cdh != null) {
            cdh.destroy();
            cdhs.invalidate(dataId);
        }

        String newDataId = DATAID_PREFIX + app + DATAID_SUFFIX;
        cdh = cdhs.getIfPresent(newDataId);
        if (cdh != null) {
            cdh.destroy();
            cdhs.invalidate(dataId);
        }

        users.remove(app);
    }

    private synchronized void loadSchema(final String app) {
        SchemaConfig schema = schemas.get(app);
        if (schema != null) {
            return;
        }

        // MDC.put("app", app);
        TDataSource ds = new TDataSource();
        ds.putConnectionProperties(ConnectionProperties.CHOOSE_STREAMING, true);
        ds.putConnectionProperties(ConnectionProperties.PROCESS_AUTO_INCREMENT_BY_SEQUENCE, true);
        ds.putConnectionProperties(ConnectionProperties.INIT_CONCURRENT_POOL_EVERY_CONNECTION, false);
        // 共享一个链接池
        ds.setGlobalExecutorService(CobarServer.getInstance().getServerExectuor());
        // ds.putConnectionProperties(ConnectionProperties.MERGE_CONCURRENT,
        // true);
        // ds.putConnectionProperties(ConnectionProperties.CONCURRENT_THREAD_SIZE,
        // CobarServer.getInstance()
        // .getConfig()
        // .getSystem()
        // .getServerExecutor());
        ds.setSharding(false);// 允许非sharding启动
        ds.setAppName(app);
        // try {
        // ds.init();
        // } catch (TddlException e) {
        // // 启动时出错不往上抛
        // logger.error(e);
        // } finally {
        // MDC.remove("app");
        // }
        schema = new SchemaConfig(app);
        schema.setDataSource(ds);
        schemas.put(app, schema);
    }

    private synchronized void unLoadSchema(final String app) throws TddlException {
        SchemaConfig schema = schemas.remove(app);
        if (schema != null) {
            TDataSource dataSource = schema.getDataSource();
            if (dataSource != null) {
                dataSource.destroy();
            }
        }
    }

    private ConfigDataHandler getQuarantineDataHandler(final String app, final String dataId) throws ExecutionException {
        return cdhs.get(dataId, new Callable<ConfigDataHandler>() {

            @Override
            public ConfigDataHandler call() throws Exception {
                return cdhf.getConfigDataHandler(dataId, new ConfigDataListener() {

                    @Override
                    public void onDataRecieved(String dataId, String data) {
                        parseQuarantineConfig(dataId, app, data);
                    }
                });
            }
        });
    }

    private ConfigDataHandler getConfigDataHandler(final String app, final String dataId) throws ExecutionException {
        return cdhs.get(dataId, new Callable<ConfigDataHandler>() {

            @Override
            public ConfigDataHandler call() throws Exception {
                return cdhf.getConfigDataHandler(dataId, new ConfigDataListener() {

                    @Override
                    public void onDataRecieved(String dataId, String data) {
                        parseConfig(dataId, app, data);
                    }
                });
            }
        });
    }

    private synchronized void parseConfig(String dataId, String app, String config) {
        if (StringUtil.isEmpty(config)) {
            logger.info("password data is empty, so use default passwd, dataId:" + dataId);
            config = app;
        }

        UserConfig user = this.users.get(app);
        if (user == null) {
            user = new UserConfig();
            this.users.put(app, user);
        }
        user.setName(app);
        user.setPassword(config);
        user.setSchemas(this.buildSchema(app));
    }

    private void parseQuarantineConfig(String dataId, String app, String config) {
        /* Must clear existing whitelist first always */
        cleanHostByApp(app);
        if (StringUtil.isEmpty(config)) {
            return;
        }

        parseQuarantine(app, dataId, config);
    }

    synchronized private void parseQuarantine(String app, String dataId, String config) {
        quarantine.resetHosts(app, config);
    }

    private void cleanHostByApp(String app) {
        quarantine.cleanHostByApp(app);
    }

    private Set<String> buildSchema(String app) {
        Set<String> result = new HashSet<String>();
        result.add(app);
        return result;
    }

    public Map<String, SchemaConfig> getSchemas() {
        return schemas;
    }

    public Map<String, UserConfig> getUsers() {
        return users;
    }

    @Override
    protected void doDestroy() throws TddlException {
        for (ConfigDataHandler cdh : cdhs.asMap().values()) {
            try {
                cdh.destroy();
            } catch (Exception e) {
                // ignore
            }
        }

        for (SchemaConfig schema : schemas.values()) {
            TDataSource dataSource = schema.getDataSource();
            try {
                if (dataSource != null) {
                    dataSource.destroy();
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }

    public QuarantineConfig getQuarantine() {
        return quarantine;
    }

}
