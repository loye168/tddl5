package com.alibaba.cobar.config.loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.cobar.config.QuarantineConfig;
import com.alibaba.cobar.net.util.TimeUtil;
import com.alibaba.cobar.server.util.StringUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.ConfigDataHandlerCity;

/**
 * 一个cluster的加载
 * 
 * @author jianghang 2014-5-28 下午5:32:40
 * @since 5.1.0
 */
public final class ClusterLoader extends AbstractLifecycle implements Lifecycle {

    private static final Logger              logger           = LoggerFactory.getLogger(ClusterLoader.class);
    private static final String              DATAID_PREFIX    = "com.taobao.corona.";
    private String                           cluster;
    private String                           dataId;
    private Set<String>                      loadedApps;
    private AppLoader                        appLoader;
    private static final String              BLACKLIST_SURFIX = ".blacklist";
    private ConfigDataHandlerFactory         cdhf             = ConfigDataHandlerCity.getFactory(null, null);
    private Cache<String, ConfigDataHandler> cdhs             = CacheBuilder.newBuilder().build();
    private QuarantineConfig                 quarantine;

    public ClusterLoader(String cluster){
        this.cluster = cluster;
        this.dataId = DATAID_PREFIX + this.cluster;
        this.loadedApps = new HashSet<String>();
        this.appLoader = new AppLoader(this.cluster);
        this.quarantine = QuarantineConfig.getInstance();
    }

    @Override
    public void doInit() throws TddlException {
        this.appLoader.init();
        this.loadCluster();
    }

    public void loadCluster() {
        try {
            this.loadApps();
            this.loadClusterBlacklist();
        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    private void loadApps() throws ExecutionException {
        ConfigDataHandler cdh = getConfigDataHandler(dataId);
        String config = cdh.getData();
        this.parseConfig(config);
    }

    private void loadClusterBlacklist() throws ExecutionException {
        String dataId = this.dataId + BLACKLIST_SURFIX;
        ConfigDataHandler cdh = getClusterQuarantineDataHandler(dataId);
        String config = cdh.getData();
        this.parseClusterBlacklist(config);
    }

    private ConfigDataHandler getConfigDataHandler(final String dataId) throws ExecutionException {
        return cdhs.get(dataId, new Callable<ConfigDataHandler>() {

            @Override
            public ConfigDataHandler call() throws Exception {
                return cdhf.getConfigDataHandler(dataId, new ConfigDataListener() {

                    @Override
                    public void onDataRecieved(String dataId, String data) {
                        parseConfig(data);
                    }
                });
            }
        });
    }

    private ConfigDataHandler getClusterQuarantineDataHandler(final String dataId) throws ExecutionException {
        return cdhs.get(dataId, new Callable<ConfigDataHandler>() {

            @Override
            public ConfigDataHandler call() throws Exception {
                return cdhf.getConfigDataHandler(dataId, new ConfigDataListener() {

                    @Override
                    public void onDataRecieved(String dataId, String data) {
                        parseClusterBlacklist(data);
                    }
                });
            }
        });
    }

    private synchronized void parseClusterBlacklist(String config) {
        quarantine.resetBlackList(config);
    }

    private synchronized void parseConfig(String config) {
        // config: app1,app2,...
        if (StringUtil.isEmpty(config)) {
            if (loadedApps.isEmpty()) {
                logger.info("this cluster has no app, tddl start normally");
            }
            for (String app : loadedApps) {// 卸载所有app
                appLoader.unLoadApp(app);
            }
            return;
        }

        logger.info("start loading apps in cluster:" + this.cluster);
        long startTime = TimeUtil.currentTimeMillis();
        List<String> appList = Arrays.asList(StringUtil.split(config, ',', true));
        // 先添加不存在的app
        for (String app : appList) {
            if (!this.loadedApps.contains(app)) {
                try {
                    this.appLoader.loadApp(app);
                    // 该APP的所有配置加载成功
                    this.loadedApps.add(app);
                } catch (Exception e) {
                    // 一个app加载失败,不能影响其他app的加载
                    logger.error("load app error:" + app, e);
                }
            }
        }

        List<String> appHistory = new ArrayList<String>(loadedApps);
        for (String app : appHistory) {
            if (!appList.contains(app)) {
                try {
                    this.appLoader.unLoadApp(app);
                    this.loadedApps.remove(app);
                } catch (Exception e) {
                    // 一个app加载失败,不能影响其他app的加载
                    logger.error("unLoad app error:" + app, e);
                }
            }
        }

        long endTime = TimeUtil.currentTimeMillis();
        logger.info("finish loading apps in cluster with " + (endTime - startTime) + "ms");
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

        for (String app : loadedApps) {
            appLoader.unLoadApp(app); // 卸载所有的app
        }
    }

    public AppLoader getAppLoader() {
        return appLoader;
    }

    public QuarantineConfig getQuarantine() {
        return quarantine;
    }

}
