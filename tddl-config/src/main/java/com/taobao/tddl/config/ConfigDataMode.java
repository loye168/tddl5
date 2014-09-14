package com.taobao.tddl.config;

import org.apache.commons.lang.StringUtils;

/**
 * 配置加载方式
 * 
 * @author jianghang 2014-5-26 下午11:53:08
 * @since 5.1.0
 */
public class ConfigDataMode {

    public static final String CONFIG_MODE = "tddl.config.mode";
    private static Mode        mode;
    static {
        String m = System.getProperty(CONFIG_MODE, "auto");
        mode = Mode.nameOf(m);
        if (mode == null) {
            mode = Mode.AUTO;
        }
    }

    public static enum Mode {
        DIAMOND("diamond"), MOCK("mock"), AUTO(null);

        private String extensionName;

        Mode(String extensionName){
            this.extensionName = extensionName;
        }

        public static Mode nameOf(String m) {
            for (Mode mode : Mode.values()) {
                if (StringUtils.equalsIgnoreCase(mode.name(), m)) {
                    return mode;
                }
            }

            return null;
        }

        public String getExtensionName() {
            return extensionName;
        }

        public boolean isMock() {
            return this == Mode.MOCK;
        }

    }

    public static Mode getMode() {
        return mode;
    }

    public static void setMode(Mode mode) {
        ConfigDataMode.mode = mode;
    }

}
