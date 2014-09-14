package com.taobao.tddl.repo.mysql.function;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.utils.PackageUtils;
import com.taobao.tddl.optimizer.utils.PackageUtils.ClassFilter;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author mengshi.sunmengshi 2014年4月8日 下午3:17:09
 * @since 5.1.0
 */
public class FunctionStringConstructorManager {

    private static final Logger          logger         = LoggerFactory.getLogger(FunctionStringConstructorManager.class);
    private static Map<String, Class<?>> functionCaches = Maps.newConcurrentMap();

    static {
        initFunctions();
    }

    public FunctionStringConstructor getConstructor(IFunction func) {
        return getExtraFunction(func.getFunctionName());
    }

    /**
     * 查找对应名字的函数类，忽略大小写
     * 
     * @param functionName
     * @return
     */
    public static FunctionStringConstructor getExtraFunction(String functionName) {
        String name = functionName;
        Class clazz = functionCaches.get(name);
        FunctionStringConstructor result = null;

        if (clazz == null) {
            return null;
        }

        if (clazz != null) {
            try {
                result = (FunctionStringConstructor) clazz.newInstance();
            } catch (Exception e) {
                throw new TddlNestableRuntimeException("init function failed", e);
            }
        }

        if (result == null) {
            throw new TddlNestableRuntimeException("not found Function : " + functionName);
        }

        return result;
    }

    public static void addFuncion(Class clazz) {

        try {
            FunctionStringConstructor sample = (FunctionStringConstructor) clazz.newInstance();

            String[] names = sample.getFunctionNames();

            for (String name : names) {
                Class oldClazz = functionCaches.put(name.toUpperCase(), clazz);
                if (oldClazz != null) {
                    logger.warn(" dup function :" + name + ", old class : " + oldClazz.getName());
                }
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException("init function failed", e);
        }

    }

    private static void initFunctions() {
        List<Class> classes = Lists.newArrayList();
        // 查找默认build-in的函数

        ClassFilter filter = new ClassFilter() {

            @Override
            public boolean filter(Class clazz) {
                int mod = clazz.getModifiers();
                return !Modifier.isAbstract(mod) && !Modifier.isInterface(mod)
                       && FunctionStringConstructor.class.isAssignableFrom(clazz);
            }

            @Override
            public boolean preFilter(String classFulName) {
                return StringUtils.contains(classFulName, "function");// 包含function名字的类
            }

        };
        classes.addAll(PackageUtils.findClassesInPackage("com.taobao.tddl", filter));
        // 查找用户自定义的扩展函数
        classes.addAll(ExtensionLoader.getAllExtendsionClass(FunctionStringConstructor.class));

        for (Class clazz : classes) {
            addFuncion(clazz);
        }
    }

}
