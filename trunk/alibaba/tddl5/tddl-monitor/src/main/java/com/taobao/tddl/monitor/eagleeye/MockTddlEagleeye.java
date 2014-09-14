package com.taobao.tddl.monitor.eagleeye;

import com.taobao.tddl.common.model.SqlMetaData;
import com.taobao.tddl.common.utils.extension.Activate;

/**
 * 空实现,屏蔽对内部产品的依赖
 * 
 * @author jianghang 2014-2-28 下午4:21:14
 * @since 5.0.0
 */
@Activate(order = 1)
public class MockTddlEagleeye implements TddlEagleeye {

    @Override
    public String index(String msg) {
        return msg;
    }

    @Override
    public void startRpc(String ip, String port, String dbName, String sqlType) {
    }

    @Override
    public void endRpc(SqlMetaData sqlMetaData, Exception e) {
    }

    @Override
    public String getUserData(String key) {
        return null;
    }

    @Override
    public Object getRpcContext() {
        return null;
    }

    @Override
    public void setRpcContext(Object rpcContext) {
    }

    @Override
    public String getTraceId() {
        return null;
    }

    @Override
    public String getRpcId() {
        return null;
    }

    @Override
    public boolean isRecordSql() {
        return false;
    }

}
