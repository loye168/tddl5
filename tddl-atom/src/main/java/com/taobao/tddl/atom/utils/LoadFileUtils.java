package com.taobao.tddl.atom.utils;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.Statement;
import java.util.concurrent.Callable;

import com.alibaba.druid.pool.DruidPooledStatement;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.common.jdbc.IStatement;

public class LoadFileUtils {

    private static Cache<Class, Method> setMethodCaches = CacheBuilder.newBuilder().build();
    private static Cache<Class, Method> getMethodCaches = CacheBuilder.newBuilder().build();

    public static void setLocalInfileInputStream(Statement stmt, InputStream stream) {
        if (stmt instanceof IStatement) {
            ((IStatement) stmt).setLocalInfileInputStream(stream);
            return;
        }

        if (stmt instanceof DruidPooledStatement) {
            stmt = ((DruidPooledStatement) stmt).getStatement();
            setLocalInfileInputStream(stmt, stream);
            return;
        }

        try {
            final Class<?> clazz = stmt.getClass();
            Method setMethod = setMethodCaches.get(clazz, new Callable<Method>() {

                @Override
                public Method call() throws Exception {

                    Class c = clazz;
                    while (true) {
                        try {
                            Method setMethod = c.getDeclaredMethod("setLocalInfileInputStream",
                                new Class[] { InputStream.class });
                            if (setMethod != null) {
                                setMethod.setAccessible(true);
                            }
                            return setMethod;
                        } catch (Exception ex) {

                        }

                        c = c.getSuperclass();

                        if (c == null) {
                            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                                "get setLocalInfileInputStream error, clazz:" + clazz);
                        }
                    }

                }
            });

            setMethod.invoke(stmt, new Object[] { stream });
        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public static InputStream getLocalInfileInputStream(Statement stmt) {
        if (stmt instanceof IStatement) {
            return ((IStatement) stmt).getLocalInfileInputStream();
        }

        if (stmt instanceof DruidPooledStatement) {
            stmt = ((DruidPooledStatement) stmt).getStatement();
            return getLocalInfileInputStream(stmt);
        }

        try {
            final Class<?> clazz = stmt.getClass();
            Method setMethod = getMethodCaches.get(clazz, new Callable<Method>() {

                @Override
                public Method call() throws Exception {

                    Class c = clazz;
                    while (true) {
                        try {
                            Method setMethod = c.getDeclaredMethod("getLocalInfileInputStream", new Class[] {});
                            if (setMethod != null) {
                                setMethod.setAccessible(true);
                            }
                            return setMethod;
                        } catch (Exception ex) {

                        }

                        c = c.getSuperclass();

                        if (c == null) {
                            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                                "get getLocalInfileInputStream error, clazz:" + clazz);
                        }
                    }
                }
            });

            return (InputStream) setMethod.invoke(stmt, new Object[] {});
        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        }
    }
}
