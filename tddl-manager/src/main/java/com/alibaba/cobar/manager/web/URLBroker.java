package com.alibaba.cobar.manager.web;

/**
 * (created at 2010-7-20)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 * @author wenfeng.cenwf 2011-3-27
 * @author haiqing.zhuhq 2011-12-14
 */
public class URLBroker {

    public static String redirectClusterListScreen() {
        return "clusterList.htm";
    }

    public static String redirectLogInPage(String result) {
        if (result.equalsIgnoreCase("null")) {
            return "index.htm";
        } else if ("login".equals(result)) {
            return "login.htm";
        }
        return "login.htm?result=" + result;
    }

    public static String redirectIndexPage(String result) {
        if (result.equalsIgnoreCase("null")) {
            return "index.htm";
        }
        return "index.htm?result=" + result;
    }

    public static String redirectForbiddenScreen() {
        return "forbidden.htm";
    }

}
