package com.taobao.tddl.config.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.taobao.tddl.common.utils.thread.NamedThreadFactory;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;

public class FileConfigDataHandlerFactory implements ConfigDataHandlerFactory {

    private final static ConcurrentHashMap<String, ConfigDataHandler> filePath         = new ConcurrentHashMap<String, ConfigDataHandler>();
    private final static ScheduledExecutorService                     executor         = Executors.newScheduledThreadPool(4,
                                                                                           new NamedThreadFactory("configure_file_checker",
                                                                                               true));
    public static final String                                        configure_prefix = "com.taobao.tddl.";
    private String                                                    directory        = "";
    private String                                                    appName;

    public FileConfigDataHandlerFactory(String directory, String appName){
        super();
        this.directory = directory;
        this.appName = appName;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId, ConfigDataListener configDataListener) {
        String key = combineDataKey(appName, dataId);
        ConfigDataHandler configDataHandler = filePath.get(key);
        if (configDataHandler == null) {
            synchronized (this) {
                configDataHandler = filePath.get(key);
                // dcl
                if (configDataHandler == null) {
                    configDataHandler = new FileConfigDataHandler(appName,
                        executor,
                        configure_prefix,
                        directory,
                        dataId,
                        configDataListener);
                    ConfigDataHandler tempCdh = filePath.putIfAbsent(key, configDataHandler);
                    if (tempCdh != null) {
                        configDataHandler = tempCdh;
                    }
                }

            }
        }
        configDataHandler.addListener(configDataListener, null);
        return configDataHandler;
    }

    private final String combineDataKey(String appName, String dataId) {
        return appName + "_" + dataId;
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId) {
        return this.getConfigDataHandler(dataId, null);
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId, List<ConfigDataListener> configDataListenerList,
                                                  Executor executor, Map<String, Object> config) {
        return this.getConfigDataHandler(dataId, null);
    }
}
