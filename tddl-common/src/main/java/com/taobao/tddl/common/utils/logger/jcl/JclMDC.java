package com.taobao.tddl.common.utils.logger.jcl;

import java.util.Map;

import com.taobao.tddl.common.utils.logger.MDCAdapter;

public class JclMDC implements MDCAdapter {

    @Override
    public void put(String key, String val) {
    }

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public void remove(String key) {
    }

    @Override
    public void clear() {

    }

    @Override
    public Map getCopyOfContextMap() {
        return null;
    }

    @Override
    public void setContextMap(Map contextMap) {
    }
}
