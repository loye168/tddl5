/**
 * (created at 2011-10-24)
 */
package com.alibaba.cobar.server.util;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class ArrayUtil {

    public static boolean contains(String[] list, String str) {
        if (list == null) return false;
        for (String string : list) {
            if (StringUtil.equals(str, string)) {
                return true;
            }
        }
        return false;
    }

}
