package com.taobao.tddl.monitor.logger;

import java.io.File;

import org.apache.commons.lang.StringUtils;

/**
 * 动态增加logger的抽象接口，运行时动态选择子类进行使用
 * 
 * @author jianghang 2013-10-24 下午6:21:07
 * @since 5.0.0
 */
public abstract class DynamicLogger {

    private String encode = "UTF-8";

    protected static String getLogPath() {
        String rootLog = System.getProperty("JM.LOG.PATH");
        if (StringUtils.isEmpty(rootLog)) {
            rootLog = System.getProperty("user.home");
        }

        if (!rootLog.endsWith(File.separator)) {
            rootLog += File.separator;
        }
        StringBuilder path = new StringBuilder();
        path.append(rootLog).append("logs").append(File.separator);
        path.append("tddl").append(File.separator);
        String file = path.toString();
        File dir = new File(file);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return file;
    }

    protected String getEncoding() {
        return encode != null ? encode : System.getProperty("file.encoding", "UTF-8");
    }

    public abstract void init();

    public void setEncode(String encode) {
        this.encode = encode;
    }
}
