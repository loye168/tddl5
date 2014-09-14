package com.taobao.tddl.common.utils.extension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 包装{@linkplain ServiceLoader}，提供SPI的获取方式 <br/>
 * jdk自带的{@linkplain ServiceLoader}
 * 存在一些局限，无法为SPI扩展定义排序规则，如果classpath存在多个SPI配置时，加载无法确定，所以包装一下允许以后替换实现
 * 
 * @see <a
 * href="http://java.sun.com/j2se/1.5.0/docs/guide/jar/jar.html#Service%20Provider">JDK5.0的自动发现机制实现</a>
 * @author jianghang 2013-9-13 下午3:38:30
 * @since 5.0.0
 */
public class ExtensionLoader<S> {

    private static final Logger            logger             = LoggerFactory.getLogger(ExtensionLoader.class);
    private static final String            SERVICES_DIRECTORY = "META-INF/services/";
    private static final String            TDDL_DIRECTORY     = "META-INF/tddl/";
    private static Map<Class, List<Class>> providers          = Maps.newConcurrentMap();

    /**
     * 指定classloader加载server provider
     * 
     * @param service
     * @param loader
     * @return
     * @throws ExtensionNotFoundException
     */
    public static <S> S load(Class<S> service, ClassLoader loader) throws ExtensionNotFoundException {
        return loadFile(service, null, loader);
    }

    /**
     * 加载server provider
     * 
     * @param service
     * @return
     * @throws ExtensionNotFoundException
     */
    public static <S> S load(Class<S> service) throws ExtensionNotFoundException {
        return loadFile(service, null, findClassLoader());
    }

    /**
     * 加载server provider
     * 
     * @param service
     * @return
     * @throws ExtensionNotFoundException
     */
    public static <S> S load(Class<S> service, String activateName) throws ExtensionNotFoundException {
        return loadFile(service, activateName, findClassLoader());
    }

    /**
     * 指定classloader加载server provider
     * 
     * @param service
     * @param loader
     * @return
     * @throws ExtensionNotFoundException
     */
    public static <S> S load(Class<S> service, String activateName, ClassLoader loader)
                                                                                       throws ExtensionNotFoundException {
        return loadFile(service, activateName, loader);
    }

    /**
     * 获取所有的扩展类，按照{@linkplain Activate}定义的order顺序进行排序
     * 
     * @return
     */
    public static <S> List<Class> getAllExtendsionClass(Class<S> service) {
        return findAllExtensionClass(service, null, findClassLoader());
    }

    /**
     * 获取所有的扩展类，按照{@linkplain Activate}定义的order顺序进行排序
     * 
     * @return
     */
    public static <S> List<Class> getAllExtendsionClass(Class<S> service, ClassLoader loader) {
        return findAllExtensionClass(service, null, loader);
    }

    private static <S> S loadFile(Class<S> service, String activateName, ClassLoader loader) {
        try {
            boolean foundFromCache = true;
            List<Class> extensions = providers.get(service);
            if (extensions == null) {
                synchronized (service) {
                    extensions = providers.get(service);
                    if (extensions == null) {
                        extensions = findAllExtensionClass(service, activateName, loader);
                        foundFromCache = false;
                        providers.put(service, extensions);
                    }
                }
            }

            // 为避免被覆盖，每个activateName的查找，允许再加一层子目录
            if (StringUtils.isNotEmpty(activateName)) {
                loadFile(service, TDDL_DIRECTORY + activateName.toLowerCase() + "/", loader, extensions);

                List<Class> activateExtensions = new ArrayList<Class>();
                for (int i = 0; i < extensions.size(); i++) {
                    Class clz = extensions.get(i);
                    Activate activate = (Activate) clz.getAnnotation(Activate.class);
                    if (activate != null && activateName.equals(activate.name())) {
                        activateExtensions.add(clz);
                    }
                }

                extensions = activateExtensions;
            }

            if (extensions.isEmpty()) {
                throw new ExtensionNotFoundException("not found service provider for : " + service.getName() + "["
                                                     + activateName + "] and classloader : "
                                                     + ObjectUtils.toString(loader));
            }
            Class<?> extension = extensions.get(extensions.size() - 1);// 最大的一个
            S result = service.cast(extension.newInstance());
            if (!foundFromCache && logger.isInfoEnabled()) {
                logger.info("load " + service.getSimpleName() + "[" + activateName + "] extension by class["
                            + extension.getName() + "]");
            }
            return result;
        } catch (Throwable e) {
            if (e instanceof ExtensionNotFoundException) {
                throw (ExtensionNotFoundException) e;
            } else {
                throw new ExtensionNotFoundException("not found service provider for : " + service.getName()
                                                     + " caused by " + ExceptionUtils.getFullStackTrace(e));
            }
        }
    }

    private static <S> List<Class> findAllExtensionClass(Class<S> service, String activateName, ClassLoader loader) {
        List<Class> extensions = Lists.newArrayList();
        try {
            loadFile(service, SERVICES_DIRECTORY, loader, extensions);
            loadFile(service, TDDL_DIRECTORY, loader, extensions);
        } catch (IOException e) {
            throw new TddlNestableRuntimeException(e);
        }

        if (extensions.isEmpty()) {
            return extensions;
        }

        // 做一下排序
        Collections.sort(extensions, new Comparator<Class>() {

            public int compare(Class c1, Class c2) {
                Integer o1 = 0;
                Integer o2 = 0;
                Activate a1 = (Activate) c1.getAnnotation(Activate.class);
                Activate a2 = (Activate) c2.getAnnotation(Activate.class);

                if (a1 != null) {
                    o1 = a1.order();
                }

                if (a2 != null) {
                    o2 = a2.order();
                }

                return o1.compareTo(o2);

            }
        });

        return extensions;
    }

    private static void loadFile(Class<?> service, String dir, ClassLoader classLoader, List<Class> extensions)
                                                                                                               throws IOException {
        String fileName = dir + service.getName();
        Enumeration<java.net.URL> urls;
        if (classLoader != null) {
            urls = classLoader.getResources(fileName);
        } else {
            urls = ClassLoader.getSystemResources(fileName);
        }

        if (urls != null) {
            while (urls.hasMoreElements()) {
                java.net.URL url = urls.nextElement();
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new InputStreamReader(url.openStream(), "utf-8"));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        final int ci = line.indexOf('#');
                        if (ci >= 0) {
                            line = line.substring(0, ci);
                        }
                        line = line.trim();
                        if (line.length() > 0) {
                            extensions.add(Class.forName(line, true, classLoader));
                        }
                    }
                } catch (ClassNotFoundException e) {
                    // ignore
                } catch (Throwable e) {
                    logger.warn(e);// 记录一下失败日志
                } finally {
                    reader.close();
                }
            }
        }
    }

    private static ClassLoader findClassLoader() {
        // 不能使用TCCL,在pandora容器中会导致无法加载plugin中的类
        return ExtensionLoader.class.getClassLoader();
    }

}
