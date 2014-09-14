package com.alibaba.cobar;

import java.util.Map;

import com.alibaba.cobar.config.QuarantineConfig;
import com.alibaba.cobar.config.SchemaConfig;
import com.alibaba.cobar.config.SystemConfig;
import com.alibaba.cobar.config.UserConfig;
import com.alibaba.cobar.config.loader.ClusterLoader;
import com.alibaba.cobar.config.loader.ServerLoader;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;

/**
 * @author xianmao.hexm
 */
public class CobarConfig extends AbstractLifecycle implements Lifecycle {

    private volatile ServerLoader  serverLoader;
    private volatile ClusterLoader clusterLoader;

    public CobarConfig(){
        serverLoader = new ServerLoader();
        try {
            serverLoader.init();
        } catch (TddlException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    @Override
    protected void doInit() throws TddlException {
        clusterLoader = new ClusterLoader(serverLoader.getSystem().getClusterName());
        clusterLoader.init();
    }

    @Override
    protected void doDestroy() throws TddlException {
        clusterLoader.destroy();
        serverLoader.destroy();
    }

    public QuarantineConfig getQuarantine() {
        return clusterLoader.getAppLoader().getQuarantine();
    }

    public QuarantineConfig getClusterQuarantine() {
        return clusterLoader.getQuarantine();
    }

    public SystemConfig getSystem() {
        return serverLoader.getSystem();
    }

    public Map<String, UserConfig> getUsers() {
        return clusterLoader.getAppLoader().getUsers();
    }

    public Map<String, SchemaConfig> getSchemas() {
        return clusterLoader.getAppLoader().getSchemas();
    }

}
