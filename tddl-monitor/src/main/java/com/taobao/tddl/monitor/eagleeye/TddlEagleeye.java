package com.taobao.tddl.monitor.eagleeye;

import com.taobao.tddl.common.model.SqlMetaData;

public interface TddlEagleeye {

    /**
     * execute之前写日志
     * 
     * @param datasourceWrapper
     * @param sqlType
     * @throws Exception
     */
    public void startRpc(String ip, String port, String dbName, String sqlType);

    /**
     * 生成index缩略信息
     * 
     * @param msg
     * @return
     */
    public String index(String msg);

    /**
     * @param sqlMetaData
     * @param e
     */
    public void endRpc(SqlMetaData sqlMetaData, Exception e);

    /**
     * 获取上下文中的数据
     * 
     * @param key
     * @return
     */
    public String getUserData(String key);

    /**
     * 获取rpc线程上下文
     * 
     * @return
     */
    public Object getRpcContext();

    /**
     * 设置rpc线程上下文
     * 
     * @param rpcContext
     */
    public void setRpcContext(Object rpcContext);

    /**
     * @return 返回traceId
     */
    public String getTraceId();

    /**
     * @return rpcId
     */
    public String getRpcId();

    /**
     * 是否统计sql
     */
    public boolean isRecordSql();
}
