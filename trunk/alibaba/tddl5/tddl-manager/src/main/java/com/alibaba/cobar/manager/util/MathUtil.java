package com.alibaba.cobar.manager.util;

/**
 * (created at 2010-9-28)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 * @author wenfeng.cenwf 2011-4-16
 * @author haiqing.zhuhq 2011-9-1
 */
public class MathUtil {

    public static double getDerivate(long newVal, long oldVal, long timestamp, long oldtimestamp, double scale) {
        return timestamp - oldtimestamp == 0 ? 0 : (newVal - oldVal) * scale / (timestamp - oldtimestamp);
    }

}
