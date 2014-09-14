package com.taobao.tddl.common.model;

public class App {

    private String appName      = null;
    private String schemaFile   = null;
    private String ruleFile     = null;
    private String topologyFile = null;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getSchemaFile() {
        return schemaFile;
    }

    public void setSchemaFile(String schemaFile) {
        this.schemaFile = schemaFile;
    }

    public String getRuleFile() {
        return ruleFile;
    }

    public void setRuleFile(String ruleFile) {
        this.ruleFile = ruleFile;
    }

    public String getTopologyFile() {
        return topologyFile;
    }

    public void setTopologyFile(String topologyFile) {
        this.topologyFile = topologyFile;
    }

    @Override
    public String toString() {
        return "App [appName=" + appName + ", schemaFile=" + schemaFile + ", ruleFile=" + ruleFile + ", topologyFile="
               + topologyFile + "]";
    }

}
