package com.taobao.tddl.config.diamond;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

import com.taobao.diamond.client.impl.DiamondEnv;
import com.taobao.diamond.client.impl.DiamondEnvRepo;
import com.taobao.diamond.client.impl.DiamondUnitSite;
import com.taobao.diamond.common.Constants;
import com.taobao.diamond.manager.ManagerListener;
import com.taobao.diamond.manager.SkipInitialCallbackListener;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.common.utils.mbean.TddlMBean;
import com.taobao.tddl.common.utils.mbean.TddlMBeanServer;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.UnitConfigDataHandler;

/**
 * 持久配置中心diamond实现
 * 
 * @author shenxun
 * @author <a href="zylicfc@gmail.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-1-11 11:22:29
 */
@Activate(name = "diamond", order = 2)
public class DiamondConfigDataHandler extends UnitConfigDataHandler {

    private static final Logger logger  = LoggerFactory.getLogger(DiamondConfigDataHandler.class);
    public static final long    TIMEOUT = 10 * 1000;
    private String              mbeanId;
    private TddlMBean           mbean;
    private DiamondEnv          env;

    @Override
    public void doInit() {
        mbean = new TddlMBean("Diamond Config Info " + System.currentTimeMillis());
        mbeanId = dataId + System.currentTimeMillis();

        // TODO 外部直接指定ip进行访问
        if (unitName != null && !"".equals(unitName.trim())) {
            env = DiamondUnitSite.getDiamondUnitEnv(unitName);
        } else {
            env = DiamondEnvRepo.defaultEnv;
        }

        if (initialData == null) {
            initialData = getData();
        }
        addListener0(listeners, (Executor) config.get("executor"), initialData);
        TddlMBeanServer.registerMBeanWithId(mbean, mbeanId);
    }

    @Override
    public String getNullableData(long timeout, String strategy) {
        String data = null;
        try {
            data = env.getConfig(dataId, null, Constants.GETCONFIG_LOCAL_SERVER_SNAPSHOT, timeout);
        } catch (IOException e) {
            // 不抛异常，只记录一下
            logger.error(e);
        }

        if (data != null) {
            mbean.setAttribute(dataId, data);
        } else {
            mbean.setAttribute(dataId, "");
        }

        return data;
    }

    @Override
    public String getData(long timeout, String strategy) {
        String data = null;
        try {
            data = env.getConfig(dataId, null, Constants.GETCONFIG_LOCAL_SERVER_SNAPSHOT, timeout);
        } catch (IOException e) {
            throw new RuntimeException("get diamond data error!dataId:" + dataId, e);
        }

        if (data != null) {
            mbean.setAttribute(dataId, data);
        } else {
            mbean.setAttribute(dataId, "");
        }

        return data;
    }

    @Override
    public void addListener(final ConfigDataListener configDataListener, final Executor executor) {
        if (configDataListener != null) {
            String data = getData();
            addListener0(configDataListener, executor, data);
        }
    }

    @Override
    public void addListeners(final List<ConfigDataListener> configDataListenerList, final Executor executor) {
        if (configDataListenerList != null) {
            String data = getData();
            addListener0(configDataListenerList, executor, data);
        }
    }

    public void closeUnderManager() {
        List<ManagerListener> listeners = env.getListeners(dataId, null);
        for (ManagerListener l : listeners) {
            env.removeListener(dataId, null, l);
        }
    }

    protected void doDestory() throws TddlException {
        closeUnderManager();
    }

    /**
     * 共用的addListener处理
     * 
     * @param configDataListenerList
     * @param executor
     * @param data
     */
    private void addListener0(final ConfigDataListener configDataListener, final Executor executor, String data) {
        env.addListeners(dataId, null, Arrays.asList(new SkipInitialCallbackListener(data) {

            @Override
            public Executor getExecutor() {
                return executor;
            }

            @Override
            public void receiveConfigInfo0(String data) {
                configDataListener.onDataRecieved(dataId, data);
                if (data != null) {
                    mbean.setAttribute(dataId, data);
                } else {
                    mbean.setAttribute(dataId, "");
                }
            }
        }));
    }

    /**
     * 共用的addListener处理
     * 
     * @param configDataListenerList
     * @param executor
     * @param data
     */
    private void addListener0(final List<ConfigDataListener> configDataListenerList, final Executor executor,
                              String data) {
        env.addListeners(dataId, null, Arrays.asList(new SkipInitialCallbackListener(data) {

            @Override
            public Executor getExecutor() {
                return executor;
            }

            @Override
            public void receiveConfigInfo0(String data) {
                for (ConfigDataListener configDataListener : configDataListenerList) {
                    try {
                        configDataListener.onDataRecieved(dataId, data);
                    } catch (Exception e) {
                        logger.error("one of listener failed", e);
                        continue;
                    }
                }

                if (data != null) {
                    mbean.setAttribute(dataId, data);
                } else {
                    mbean.setAttribute(dataId, "");
                }
            }
        }));
    }

    @Override
    public String getDataId() {
        return dataId;
    }

    @Override
    public void setDataId(String dataId) {
        this.dataId = dataId;
    }
}
