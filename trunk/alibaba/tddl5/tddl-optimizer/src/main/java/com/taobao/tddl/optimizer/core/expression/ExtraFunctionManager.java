package com.taobao.tddl.optimizer.core.expression;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;
import com.taobao.tddl.optimizer.exception.OptimizerException;
import com.taobao.tddl.optimizer.utils.PackageUtils;
import com.taobao.tddl.optimizer.utils.PackageUtils.ClassFilter;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * {@linkplain IExtraFunction}加载器，以类名做为Function Name，<strong>注意：忽略了大小写</stong>
 * 
 * <pre>
 * Function加载：
 * 1. 自动扫描IExtraFunction对应Package目录下的所有Function实现
 * 2. 自动扫描Extension扩展方式下的自定义实现，比如在META-INF/tddl 或 META-INF/services 添加扩展配置文件
 * </pre>
 * 
 * @author jianghang 2013-11-8 下午5:30:35
 * @since 5.0.0
 */
public class ExtraFunctionManager {

    private static final Logger          logger               = LoggerFactory.getLogger(ExtraFunctionManager.class);
    private static Map<String, Class<?>> functionCaches       = Maps.newConcurrentMap();
    private static String                DUMMAY_FUNCTION      = "DUMMY";
    private static String                DUMMAY_TEST_FUNCTION = "DUMMYTEST";
    private static IExtraFunction        dummyFunction;                                                             // 缓存一下dummy，避免每次都反射创建

    static {
        initFunctions();
        dummyFunction = getExtraFunction(DUMMAY_FUNCTION);
        if (dummyFunction == null) {
            dummyFunction = getExtraFunction(DUMMAY_TEST_FUNCTION);
        }
    }

    /**
     * 查找对应名字的函数类，忽略大小写
     * 
     * @param functionName
     * @return
     */
    public static IExtraFunction getExtraFunction(String functionName) {
        String name = functionName;
        Class clazz = functionCaches.get(name);
        IExtraFunction result = null;

        if (clazz == null) {
            return dummyFunction;
        }

        if (clazz != null) {
            try {
                result = (IExtraFunction) clazz.newInstance();
            } catch (Exception e) {
                throw new TddlNestableRuntimeException(e);
            }
        }

        if (result == null) {
            throw new OptimizerException("not found Function : " + functionName);
        }

        return result;
    }

    public static void addFuncion(Class clazz) {

        try {
            IExtraFunction sample = (IExtraFunction) clazz.newInstance();
            String[] names = sample.getFunctionNames();
            for (String name : names) {
                Class oldClazz = functionCaches.put(name.toUpperCase(), clazz);
                if (oldClazz != null) {
                    logger.warn(" dup function :" + name + ", old class : " + oldClazz.getName());
                }
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
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
                       && IExtraFunction.class.isAssignableFrom(clazz);
            }

            @Override
            public boolean preFilter(String classFulName) {
                return StringUtils.contains(classFulName, "function");// 包含function名字的类
            }

        };
        classes.addAll(PackageUtils.findClassesInPackage("com.taobao.tddl", filter));
        // 查找用户自定义的扩展函数
        classes.addAll(ExtensionLoader.getAllExtendsionClass(IExtraFunction.class));

        for (Class clazz : classes) {
            addFuncion(clazz);
        }
    }

}
