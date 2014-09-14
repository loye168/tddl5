package com.taobao.tddl.monitor.logger.log4j;

import java.lang.reflect.Field;

import org.slf4j.impl.Log4jLoggerAdapter;

import com.taobao.tddl.common.exception.TddlNestableRuntimeException;

import com.taobao.tddl.common.utils.logger.Logger;

/**
 * slf4j + log4j组合方式
 * 
 * @author jianghang 2014-3-21 上午11:41:34
 * @since 5.0.4
 */
public class DynamicLog4jAdapterLogger extends DynamicLog4jLogger {

    private static Field log4jField = null;
    static {
        try {
            Field[] fields = Log4jLoggerAdapter.class.getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                if ("logger".equals(field.getName())) {
                    log4jField = field;
                    log4jField.setAccessible(true);
                }
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    @Override
    protected org.apache.log4j.Logger getLog4jLogger(Logger logger) {
        Log4jLoggerAdapter adapter = (Log4jLoggerAdapter) logger.getDelegate();
        try {
            return (org.apache.log4j.Logger) log4jField.get(adapter);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }
}
