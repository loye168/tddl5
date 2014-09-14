package com.taobao.tddl.atom.utils;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.concurrent.Callable;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.atom.jdbc.TConnectionWrapper;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.common.jdbc.IConnection;

public class EncodingUtils {

    private static Cache<Class, Method> setMethodCaches = CacheBuilder.newBuilder().build();
    private static Cache<Class, Method> getMethodCaches = CacheBuilder.newBuilder().build();

    public static String mysqlEncoding(String encoding) {
        if (encoding.equalsIgnoreCase("iso_8859_1")) {
            return "latin1";
        }

        if (encoding.equalsIgnoreCase("utf-8")) {
            return "utf8";
        }

        return encoding;
    }

    public static String javaEncoding(String encoding) {
        if (encoding.equalsIgnoreCase("utf8mb4")) {
            return "utf8";
        }

        return encoding;
    }

    public static void setEncoding(Connection conn, String encoding) {
        if (conn instanceof IConnection) {
            ((IConnection) conn).setEncoding(encoding);
            return;
        }

        if (conn instanceof TConnectionWrapper) {
            conn = ((TConnectionWrapper) conn).getTargetConnection();
            setEncoding(conn, encoding);
            return;
        }

        if (conn instanceof DruidPooledConnection) {
            conn = ((DruidPooledConnection) conn).getConnection();
            setEncoding(conn, encoding);
            return;
        }

        try {
            final Class<?> clazz = conn.getClass();
            Method setMethod = setMethodCaches.get(clazz, new Callable<Method>() {

                @Override
                public Method call() throws Exception {

                    Class c = clazz;
                    while (true) {
                        try {
                            Method setMethod = c.getDeclaredMethod("setEncoding", new Class[] { String.class });
                            if (setMethod != null) {
                                setMethod.setAccessible(true);
                            }
                            return setMethod;
                        } catch (Exception ex) {
                        }

                        c = c.getSuperclass();
                        if (c == null) {
                            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "get setEncoding error, clazz:"
                                                                                   + clazz);
                        }
                    }

                }
            });

            setMethod.invoke(conn, new Object[] { encoding });
        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public static String getEncoding(Connection conn) {
        if (conn instanceof IConnection) {
            return ((IConnection) conn).getEncoding();
        }

        if (conn instanceof TConnectionWrapper) {
            conn = ((TConnectionWrapper) conn).getTargetConnection();
            return getEncoding(conn);
        }

        if (conn instanceof DruidPooledConnection) {
            conn = ((DruidPooledConnection) conn).getConnection();
            return getEncoding(conn);
        }

        try {
            final Class<?> clazz = conn.getClass();
            Method getMethod = getMethodCaches.get(clazz, new Callable<Method>() {

                @Override
                public Method call() throws Exception {

                    Class c = clazz;
                    while (true) {
                        try {
                            Method setMethod = c.getDeclaredMethod("getEncoding", new Class[] {});
                            if (setMethod != null) {
                                setMethod.setAccessible(true);
                            }
                            return setMethod;
                        } catch (Exception ex) {
                        }

                        c = c.getSuperclass();

                        if (c == null) {
                            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "get getEncoding error, clazz:"
                                                                                   + clazz);
                        }
                    }
                }
            });

            return (String) getMethod.invoke(conn, new Object[] {});
        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        }
    }
}
