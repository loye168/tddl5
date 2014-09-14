package com.taobao.tddl.common.utils.logger;

import java.util.Map;

import com.taobao.tddl.common.utils.logger.jcl.JclLoggerAdapter;
import com.taobao.tddl.common.utils.logger.jcl.JclMDC;
import com.taobao.tddl.common.utils.logger.jdk.JdkMDC;
import com.taobao.tddl.common.utils.logger.log4j.Log4jLoggerAdapter;
import com.taobao.tddl.common.utils.logger.log4j.Log4jMDC;
import com.taobao.tddl.common.utils.logger.slf4j.Slf4jLoggerAdapter;
import com.taobao.tddl.common.utils.logger.slf4j.Slf4jMDC;

public class MDC {

    static MDCAdapter mdcAdapter;
    static {
        LoggerAdapter logger = LoggerFactory.getLoggerAdapter();
        if (logger instanceof Slf4jLoggerAdapter) {
            mdcAdapter = new Slf4jMDC();
        } else if (logger instanceof JclLoggerAdapter) {
            mdcAdapter = new JclMDC();
        } else if (logger instanceof Log4jLoggerAdapter) {
            mdcAdapter = new Log4jMDC();
        } else {
            mdcAdapter = new JdkMDC();
        }
    }

    public static void put(String key, String val) {
        if (mdcAdapter == null) {
            throw new IllegalStateException("MDCAdapter cannot be null. ");
        }
        mdcAdapter.put(key, val);
    }

    public static String get(String key) {
        if (mdcAdapter == null) {
            throw new IllegalStateException("MDCAdapter cannot be null. ");
        }
        return mdcAdapter.get(key);
    }

    public static void remove(String key) {
        if (mdcAdapter == null) {
            throw new IllegalStateException("MDCAdapter cannot be null. ");
        }
        mdcAdapter.remove(key);
    }

    public static void clear() {
        if (mdcAdapter == null) {
            throw new IllegalStateException("MDCAdapter cannot be null. ");
        }
        mdcAdapter.clear();
    }

    public static Map getCopyOfContextMap() {
        if (mdcAdapter == null) {
            throw new IllegalStateException("MDCAdapter cannot be null. ");
        }
        return mdcAdapter.getCopyOfContextMap();
    }

    public static void setContextMap(Map contextMap) {
        if (mdcAdapter == null) {
            throw new IllegalStateException("MDCAdapter cannot be null. ");
        }
        mdcAdapter.setContextMap(contextMap);
    }

}
