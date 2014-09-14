package com.alibaba.cobar.config;

import com.taobao.tddl.matrix.jdbc.TDataSource;
import com.taobao.tddl.statistics.SQLRecorder;

/**
 * @author xianmao.hexm
 */
public final class SchemaConfig {

    private final String name;
    private TDataSource  ds = null;
    private SQLRecorder  recorder;

    public SchemaConfig(String name){
        this.name = name;
        recorder = new SQLRecorder(100, true);
    }

    public String getName() {
        return name;
    }

    public TDataSource getDataSource() {
        return ds;
    }

    public void setDataSource(TDataSource ds) {
        this.ds = ds;
    }

    public SQLRecorder getRecorder() {
        return this.recorder;

    }

    public void setRecorder(SQLRecorder recorder) {
        this.recorder = recorder;
    }

}
