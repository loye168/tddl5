package com.taobao.tddl.config.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataListener;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class FileConfigDataHandler extends AbstractLifecycle implements ConfigDataHandler {

    private static final Logger           logger              = LoggerFactory.getLogger(FileConfigDataHandler.class);
    private final AtomicReference<String> data                = new AtomicReference<String>();
    private String                        pattern;
    private String                        directory;
    private String                        dataId;
    private String                        appName;
    private List<ConfigDataListener>      configDataListeners = new ArrayList<ConfigDataListener>();
    private ScheduledExecutorService      executor;
    private int                           period              = 20000;

    public FileConfigDataHandler(String appName, ScheduledExecutorService executor, String pattern, String directory,
                                 String dataId, ConfigDataListener configDataListener){
        super();

        this.pattern = pattern;
        this.directory = directory;
        this.dataId = dataId;
        this.appName = appName;
        this.executor = executor;
        if (configDataListener != null) {
            this.configDataListeners.add(configDataListener);
        }
        if (executor == null) {
            throw new IllegalArgumentException("executor is null");
        }

        this.executor.scheduleAtFixedRate(new CheckerTask(data,
            pattern,
            directory,
            dataId,
            configDataListeners,
            appName), period, period, TimeUnit.MILLISECONDS);
    }

    public static class CheckerTask implements Runnable {

        private AtomicReference<String>  data;
        private String                   pattern;
        private String                   directory;
        private List<ConfigDataListener> configDataListeners;
        private String                   appName;
        private String                   dataId;

        public CheckerTask(AtomicReference<String> data, String pattern, String directory, String dataId,
                           List<ConfigDataListener> configDataListeners, String appName){
            super();
            this.dataId = dataId;
            this.data = data;
            this.pattern = pattern;
            this.directory = directory;
            this.dataId = dataId;
            this.appName = appName;
            this.configDataListeners = configDataListeners;
        }

        @Override
        public void run() {
            try {
                String dataNew = getNewProperties(directory, dataId, pattern, appName);
                if (!dataNew.equalsIgnoreCase(data.get())) {// 配置变更啦
                    this.data.set(dataNew.toString());
                    // 复制一份，避免并发修改
                    List<ConfigDataListener> cdls = new ArrayList<ConfigDataListener>(configDataListeners);
                    for (ConfigDataListener cdl : cdls) {
                        cdl.onDataRecieved(dataId, data.get());
                    }
                }
            } catch (IOException e) {
                logger.error(e);
            }

        }

    }

    @Override
    public String getData(long timeout, String strategy) {
        try {
            String dataNew = getNewProperties(directory, dataId, pattern, appName);
            this.data.set(dataNew);
            return data.get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getNewProperties(String directory, String key, String pattern, String appName)
                                                                                                        throws IOException {
        StringBuilder url = getUrlWithoutDiamondPattern(directory, key);
        InputStream in = null;
        try {
            in = GeneralUtil.getInputStream(url.toString());
        } catch (Exception e) {
            logger.error("", e);
        }
        if (in == null) {
            try {
                in = GeneralUtil.getInputStream(getUrlWithDiamondPattern(directory, key, pattern, appName).toString());
            } catch (Exception e) {
                logger.error("", e);
            }
        }
        if (in == null) {
            throw new IllegalArgumentException("can't find file on " + url + " . or on "
                                               + getUrlWithDiamondPattern(directory, key, pattern, appName).toString());
        }
        try {
            BufferedReader bf = new BufferedReader(new InputStreamReader(in));
            return StringUtils.join(IOUtils.readLines(bf), System.getProperty("line.separator"));
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    private static StringBuilder getUrlWithoutDiamondPattern(String directory, String key) {
        StringBuilder url = new StringBuilder();
        if (directory != null) {
            url.append(directory);
        }
        url.append(key);
        return url;
    }

    private static StringBuilder getUrlWithDiamondPattern(String directory, String key, String pattern, String appName) {
        StringBuilder url = new StringBuilder();
        if (directory != null) {
            url.append(directory);
        }
        if (pattern != null) {
            url.append(pattern);
        }
        if (appName != null) {
            url.append(appName);
        }
        url.append(key);
        return url;
    }

    @Override
    public String getNullableData(long timeout, String strategy) {
        return this.getData(timeout, strategy);
    }

    @Override
    public void addListener(ConfigDataListener configDataListener, Executor executor) {
        if (!configDataListeners.contains(configDataListener)) {
            if (configDataListener == null) {
                return;
            }
            configDataListeners.add(configDataListener);
        }
    }

    @Override
    public void addListeners(List<ConfigDataListener> configDataListenerList, Executor executor) {
        for (ConfigDataListener l : configDataListenerList) {
            this.addListener(l, executor);
        }
    }

    @Override
    public String getData() {
        return getData(GET_DATA_TIMEOUT, FIRST_SERVER_STRATEGY);
    }

}
