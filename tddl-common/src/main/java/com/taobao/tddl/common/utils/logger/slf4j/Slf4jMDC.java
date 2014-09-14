package com.taobao.tddl.common.utils.logger.slf4j;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.common.utils.logger.MDCAdapter;

/**
 * @author jianghang 2014-6-13 上午10:53:14
 * @since 5.1.0
 */
public class Slf4jMDC implements MDCAdapter {

    @Override
    public void put(String key, String val) {
        org.slf4j.MDC.put(key, val);
    }

    @Override
    public String get(String key) {
        return org.slf4j.MDC.get(key);
    }

    @Override
    public void remove(String key) {
        org.slf4j.MDC.remove(key);
    }

    @Override
    public void clear() {
        org.slf4j.MDC.clear();
    }

    @Override
    public Map getCopyOfContextMap() {
        return org.slf4j.MDC.getCopyOfContextMap();
    }

    @Override
    public void setContextMap(Map contextMap) {
        if (contextMap == null) {
            contextMap = new HashMap();
        }
        org.slf4j.MDC.setContextMap(contextMap);
    }
}
