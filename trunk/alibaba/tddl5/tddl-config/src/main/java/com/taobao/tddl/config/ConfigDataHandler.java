package com.taobao.tddl.config;

import java.util.List;
import java.util.concurrent.Executor;

import com.taobao.tddl.common.model.lifecycle.Lifecycle;

/**
 * 获取配置的处理器
 * 
 * @author <a href="zylicfc@gmail.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-1-11上午11:22:29
 */
public interface ConfigDataHandler extends Lifecycle {

    public static final long   GET_DATA_TIMEOUT                 = 10 * 1000;
    public static final String FIRST_SERVER_STRATEGY            = "firstServer";
    public static final String FIRST_CACHE_THEN_SERVER_STRATEGY = "firstCache";

    /**
     * 从配置中心拉取数据，使用默认的超时和策略
     * 
     * @return
     */
    String getData();

    /**
     * 从配置中心拉取数据
     * 
     * @param timeout 获取配置信息超时时间
     * @param strategy 获取配置策略
     * @return
     */
    String getData(long timeout, String strategy);

    /**
     * 从配置中心拉取数据，返回结果允许为null
     * 
     * @param timeout 获取配置信息超时时间
     * @param strategy 获取配置策略
     * @return
     */
    String getNullableData(long timeout, String strategy);

    /**
     * 为推送过来的数据注册处理的监听器
     * 
     * @param configDataListener 监听器
     * @param executor 执行的executor
     */
    void addListener(ConfigDataListener configDataListener, Executor executor);

    /**
     * 为推送过来的数据注册多个处理监听器
     * 
     * @param configDataListenerList 监听器列表
     * @param executor 执行的executor
     */
    void addListeners(List<ConfigDataListener> configDataListenerList, Executor executor);

}
