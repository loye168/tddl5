package com.taobao.tddl.executor.common;

import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.executor.repo.RepositoryHolder;
import com.taobao.tddl.executor.spi.ITopologyExecutor;
import com.taobao.tddl.optimizer.sequence.ISequenceManager;

/**
 * @author mengshi.sunmengshi 2013-12-4 下午6:16:32
 * @since 5.0.0
 */
public class ExecutorContext {

    private static final String EXECUTOR_CONTEXT_KEY = "_executor_context_";
    private RepositoryHolder    repositoryHolder     = null;
    private TopologyHandler     topologyHandler      = null;
    private ITopologyExecutor   topologyExecutor     = null;
    private ISequenceManager    seqManager           = null;

    public static ExecutorContext getContext() {
        return (ExecutorContext) ThreadLocalMap.get(EXECUTOR_CONTEXT_KEY);
    }

    public static void setContext(ExecutorContext context) {
        ThreadLocalMap.put(EXECUTOR_CONTEXT_KEY, context);
    }

    public RepositoryHolder getRepositoryHolder() {
        return repositoryHolder;
    }

    public void setRepositoryHolder(RepositoryHolder repositoryHolder) {
        this.repositoryHolder = repositoryHolder;
    }

    public ITopologyExecutor getTopologyExecutor() {
        return topologyExecutor;
    }

    public void setTopologyExecutor(ITopologyExecutor topologyExecutor) {
        this.topologyExecutor = topologyExecutor;
    }

    public TopologyHandler getTopologyHandler() {
        return topologyHandler;
    }

    public void setTopologyHandler(TopologyHandler topologyHandler) {
        this.topologyHandler = topologyHandler;
    }

    public ISequenceManager getSeqeunceManager() {
        try {
            // lazy启动,兼容老系统
            if (!this.seqManager.isInited()) {
                this.seqManager.init();
            }
        } catch (TddlException e) {
            throw new TddlNestableRuntimeException(e);
        }
        return this.seqManager;
    }

    public void setSeqeunceManager(ISequenceManager seqManager) {
        this.seqManager = seqManager;
    }

}
