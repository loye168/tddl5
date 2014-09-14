package com.taobao.tddl.monitor.eagleeye;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.model.SqlMetaData;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;

/**
 * @author mengshi.sunmengshi
 */
public class EagleeyeHelper {

    static TddlEagleeye        eagleeye              = null;
    public static final String ALL_PERF_TABLE_PREFIX = "__test_";

    static {
        eagleeye = ExtensionLoader.load(TddlEagleeye.class);
    }

    /**
     * execute之前写日志
     * 
     * @param datasourceWrapper
     * @param sqlType
     * @throws Exception
     */
    public static void startRpc(String ip, String port, String dbName, String sqlType) {
        eagleeye.startRpc(ip, port, dbName, sqlType);
    }

    /**
     * 生成index缩略信息
     */
    public static String index(String sql) {
        return eagleeye.index(sql);
    }

    /**
     * @param sqlMetaData
     * @param e
     */
    public static void endRpc(SqlMetaData sqlMetaData, Exception e) {
        eagleeye.endRpc(sqlMetaData, e);
    }

    /**
     * 获取上下文中的数据
     * 
     * @param key
     * @return
     */
    public static String getUserData(String key) {
        return eagleeye.getUserData(key);
    }

    /**
     * 获取rpc线程上下文
     * 
     * @return
     */
    public static Object getRpcContext() {
        return eagleeye.getRpcContext();
    }

    /**
     * 设置rpc线程上下文
     * 
     * @param rpcContext
     */
    public static void setRpcContext(Object rpcContext) {
        eagleeye.setRpcContext(rpcContext);
    }

    /**
     * @return 返回traceId
     */
    public static String getTraceId() {
        return eagleeye.getTraceId();
    }

    /**
     * @return 返回rpcId
     */
    public static String getRpcId() {
        return eagleeye.getRpcId();
    }

    /**
     * 判断是否为全链路压测模式
     */
    public static boolean isTestMode() {
        return StringUtils.equals(EagleeyeHelper.getUserData("t"), "1");
    }

    /**
     * 判断是否需要进行sql审计
     */
    public static boolean isRecordSql() {
        return eagleeye.isRecordSql();
    }
}
