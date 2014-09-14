package com.taobao.tddl.common.client.util;

import java.util.HashMap;
import java.util.Map;

/**
 * 为了兼容老的tddl3,特别保留了client这个名字
 * 
 * @author jianghang 2013-10-24 下午2:01:31
 * @since 5.0.0
 */
public class ThreadLocalMap {

    protected final static ThreadLocal<Map<Object, Object>> threadContext = new MapThreadLocal();

    public static void put(Object key, Object value) {
        getContextMap().put(key, value);
    }

    public static Object remove(Object key) {
        return getContextMap().remove(key);
    }

    public static Object get(Object key) {
        return getContextMap().get(key);
    }

    public static boolean containsKey(Object key) {
        return getContextMap().containsKey(key);
    }

    private static class MapThreadLocal extends ThreadLocal<Map<Object, Object>> {

        protected Map<Object, Object> initialValue() {
            return new HashMap<Object, Object>() {

                private static final long serialVersionUID = 3637958959138295593L;

                public Object put(Object key, Object value) {
                    return super.put(key, value);
                }
            };
        }
    }

    /**
     * 取得thread context Map的实例。
     * 
     * @return thread context Map的实例
     */
    public static Map<Object, Object> getContextMap() {
        return (Map<Object, Object>) threadContext.get();
    }

    /**
     * 设置thread context Map的实例。
     */
    public static void setContextMap(Map<Object, Object> context) {
        threadContext.set(context);
    }

    /**
     * 清理线程所有被hold住的对象。以便重用！
     */

    public static void reset() {
        getContextMap().clear();
    }
}
