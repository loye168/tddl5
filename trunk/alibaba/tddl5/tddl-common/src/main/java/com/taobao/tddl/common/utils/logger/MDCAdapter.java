package com.taobao.tddl.common.utils.logger;

import java.util.Map;

/**
 * 适配所有logger对象的MDC
 * 
 * @author jianghang 2014-6-13 上午10:50:22
 * @since 5.1.0
 */
public interface MDCAdapter {

    /**
     * Put a context value (the <code>val</code> parameter) as identified with
     * the <code>key</code> parameter into the current thread's context map. The
     * <code>key</code> parameter cannot be null. The code>val</code> parameter
     * can be null only if the underlying implementation supports it.
     * <p>
     * If the current thread does not have a context map it is created as a side
     * effect of this call.
     */
    public void put(String key, String val);

    /**
     * Get the context identified by the <code>key</code> parameter. The
     * <code>key</code> parameter cannot be null.
     * 
     * @return the string value identified by the <code>key</code> parameter.
     */
    public String get(String key);

    /**
     * Remove the the context identified by the <code>key</code> parameter. The
     * <code>key</code> parameter cannot be null.
     * <p>
     * This method does nothing if there is no previous value associated with
     * <code>key</code>.
     */
    public void remove(String key);

    /**
     * Clear all entries in the MDC.
     */
    public void clear();

    /**
     * Return a copy of the current thread's context map, with keys and values
     * of type String. Returned value may be null.
     * 
     * @return A copy of the current thread's context map. May be null.
     * @since 1.5.1
     */
    public Map getCopyOfContextMap();

    /**
     * Set the current thread's context map by first clearing any existing map
     * and then copying the map passed as parameter. The context map parameter
     * must only contain keys and values of type String.
     * 
     * @param contextMap must contain only keys and values of type String
     * @since 1.5.1
     */
    public void setContextMap(Map contextMap);
}
