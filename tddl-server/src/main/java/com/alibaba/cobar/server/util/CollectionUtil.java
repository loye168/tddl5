/**
 * (created at 2011-10-24)
 */
package com.alibaba.cobar.server.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class CollectionUtil {

    /**
     * @param orig if null, return intersect
     */
    public static Set<? extends Object> intersectSet(Set<? extends Object> orig, Set<? extends Object> intersect) {
        if (orig == null) return intersect;
        if (intersect == null || orig.isEmpty()) return Collections.emptySet();
        Set<Object> set = new HashSet<Object>(orig.size());
        for (Object p : orig) {
            if (intersect.contains(p)) set.add(p);
        }
        return set;
    }
}
