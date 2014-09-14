package com.taobao.tddl.common.exception.code;

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.TddlRuntimeException;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * <pre>
 * ResourceBundle工具类
 * 不同的资源文件,创建不同的 ResourceBundleUtil,从该资源文件中,得到key对应的描述信息
 * 
 * 使用方法:
 * 
 * 资源文件 res/BundleName.properties内容如下:
 * key1=value1
 * key2=value2,{0}
 * 
 * 代码:
 * ResourceBundleUtil util = new ResourceBundleUtil("res/BundleName");
 * util.getMessage("key1");                   //输出:value1
 * util.getMessage("key2","stone");           //输出:value2,stone
 * </pre>
 * 
 * @author jianghang 2014-3-20 下午6:02:00
 * @since 5.0.4
 */
public class ResourceBundleUtil {

    private static final Logger             logger                          = LoggerFactory.getLogger(ResourceBundleUtil.class);
    private static final ResourceBundleUtil instance                        = new ResourceBundleUtil("res/ErrorCode");
    public static final String              DEFAULT_PLACEHOLDER_PREFIX      = "${";
    public static final String              DEFAULT_PLACEHOLDER_SUFFIX      = "}";
    public static final int                 SYSTEM_PROPERTIES_MODE_NEVER    = 0;
    public static final int                 SYSTEM_PROPERTIES_MODE_FALLBACK = 1;
    public static final int                 SYSTEM_PROPERTIES_MODE_OVERRIDE = 2;

    private String                          placeholderPrefix               = DEFAULT_PLACEHOLDER_PREFIX;
    private String                          placeholderSuffix               = DEFAULT_PLACEHOLDER_SUFFIX;
    private int                             systemPropertiesMode            = 1;
    private boolean                         ignoreUnresolvablePlaceholders  = false;
    private ResourceBundle                  bundle;                                                                             // 资源

    public static ResourceBundleUtil getInstance() {
        return instance;
    }

    /**
     * 构建ResourceBundleUtil,初始化bundle
     * 
     * @param bundleName 资源名
     * @throws MissingResourceException 资源文件不存在,则抛出运行期异常
     */
    public ResourceBundleUtil(String bundleName){
        this.bundle = ResourceBundle.getBundle(bundleName);
    }

    /**
     * <pre>
     * 从资源文件中,根据key,得到详细描述信息
     * 资源文件中,key对应的message允许占位符,根据params组成动态的描述信息
     * 
     * 如果key为null,则返回null
     * 如果key对应的message为null,则返回null
     * </pre>
     * 
     * @param key 详细描述对应的关键词
     * @param params 占位符对应的内容
     * @return 详细描述信息
     */
    public String getMessage(String key, int code, String type, String... params) {
        // 参数校验
        if (key == null) {
            return null;
        }
        // 得到message内容
        String msg = bundle.getString(key);
        msg = parseStringValue(msg, bundle, new HashSet<String>());
        msg = StringUtils.replace(msg, "{code}", String.valueOf(code));
        msg = StringUtils.replace(msg, "{type}", String.valueOf(type));
        // 如果不存在动态内容,则直接返回msg
        if (params == null || params.length == 0) {
            return msg;
        }
        // 存在动态内容,渲染后返回新的message
        if (StringUtils.isBlank(msg)) {
            // 如果得到的msg为null或者空字符串,则直接返回msg本身
            return msg;
        }
        return MessageFormat.format(msg, (Object[]) params);
    }

    protected String parseStringValue(String strVal, ResourceBundle bundle, Set<String> visitedPlaceholders) {
        StringBuffer buf = new StringBuffer(strVal);
        int startIndex = strVal.indexOf(placeholderPrefix);
        while (startIndex != -1) {
            int endIndex = findPlaceholderEndIndex(buf, startIndex);
            if (endIndex != -1) {
                String placeholder = buf.substring(startIndex + placeholderPrefix.length(), endIndex);
                if (!visitedPlaceholders.add(placeholder)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Circular placeholder reference '"
                                                                         + placeholder + "' in bundle definitions");
                }

                placeholder = parseStringValue(placeholder, bundle, visitedPlaceholders);
                // Now obtain the value for the fully resolved key...
                String propVal = resolvePlaceholder(placeholder, bundle, this.systemPropertiesMode);
                if (propVal != null) {
                    propVal = parseStringValue(propVal, bundle, visitedPlaceholders);
                    buf.replace(startIndex, endIndex + this.placeholderSuffix.length(), propVal);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Resolved placeholder '" + placeholder + "'");
                    }
                    startIndex = buf.indexOf(this.placeholderPrefix, startIndex + propVal.length());
                } else if (this.ignoreUnresolvablePlaceholders) {
                    // Proceed with unprocessed value.
                    startIndex = buf.indexOf(this.placeholderPrefix, endIndex + this.placeholderSuffix.length());
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Could not resolve placeholder '"
                                                                         + placeholder + "'");
                }
                visitedPlaceholders.remove(placeholder);
            } else {
                startIndex = -1;
            }
        }

        return buf.toString();
    }

    private int findPlaceholderEndIndex(CharSequence buf, int startIndex) {
        int index = startIndex + placeholderPrefix.length();
        int withinNestedPlaceholder = 0;
        while (index < buf.length()) {
            if (substringMatch(buf, index, placeholderSuffix)) {
                if (withinNestedPlaceholder > 0) {
                    withinNestedPlaceholder--;
                    index = index + placeholderSuffix.length();
                } else {
                    return index;
                }
            } else if (substringMatch(buf, index, placeholderPrefix)) {
                withinNestedPlaceholder++;
                index = index + placeholderPrefix.length();
            } else {
                index++;
            }
        }
        return -1;
    }

    private boolean substringMatch(CharSequence str, int index, CharSequence substring) {
        for (int j = 0; j < substring.length(); j++) {
            int i = index + j;
            if (i >= str.length() || str.charAt(i) != substring.charAt(j)) {
                return false;
            }
        }
        return true;
    }

    private String resolvePlaceholder(String placeholder, ResourceBundle bundle, int systemPropertiesMode) {
        String propVal = null;
        if (systemPropertiesMode == SYSTEM_PROPERTIES_MODE_OVERRIDE) {
            propVal = resolveSystemProperty(placeholder);
        }
        if (propVal == null) {
            propVal = resolvePlaceholder(placeholder, bundle);
        }
        if (propVal == null && systemPropertiesMode == SYSTEM_PROPERTIES_MODE_FALLBACK) {
            propVal = resolveSystemProperty(placeholder);
        }
        return propVal;
    }

    protected String resolvePlaceholder(String placeholder, ResourceBundle bundle) {
        return bundle.getString(placeholder);
    }

    private String resolveSystemProperty(String key) {
        try {
            String value = System.getProperty(key);
            if (value == null) {
                value = System.getenv(key);
            }
            return value;
        } catch (Throwable ex) {
            if (logger.isDebugEnabled()) {
                logger.debug("Could not access system property '" + key + "': " + ex);
            }
            return null;
        }
    }

}
