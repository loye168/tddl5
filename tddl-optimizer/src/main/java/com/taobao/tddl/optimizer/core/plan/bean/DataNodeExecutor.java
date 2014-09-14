package com.taobao.tddl.optimizer.core.plan.bean;

import java.util.Collection;

import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public abstract class DataNodeExecutor<RT extends IDataNodeExecutor> implements IDataNodeExecutor<RT> {

    protected String      requestHostName;
    protected Long        requestId;
    protected Long        subRequestId;
    protected String      targetNode;
    protected boolean     consistentRead   = true;
    protected Integer     thread;
    protected Object      extra;
    protected boolean     useBIO           = false;
    protected String      sql;
    protected boolean     streaming        = false;
    protected boolean     lazyLoad         = false;
    protected boolean     existSequenceVal = false; // 是否存在sequence
    protected Long        lastSequenceVal  = 0L;   // 上一次生成的sequenceVal
    protected ExplainMode explainMode      = null;

    @Override
    public RT executeOn(String targetNode) {
        this.targetNode = targetNode;
        return (RT) this;
    }

    @Override
    public boolean getConsistent() {
        return consistentRead;
    }

    @Override
    public Object getExtra() {
        return this.extra;
    }

    @Override
    public String getDataNode() {
        return targetNode;
    }

    @Override
    public String getRequestHostName() {
        return requestHostName;
    }

    @Override
    public Long getRequestId() {
        return requestId;
    }

    @Override
    public String getSql() {
        return this.sql;
    }

    @Override
    public Long getSubRequestId() {
        return requestId;
    }

    @Override
    public Integer getThread() {
        return this.thread;
    }

    @Override
    public boolean isStreaming() {
        return this.streaming;
    }

    @Override
    public boolean isUseBIO() {
        return this.useBIO;
    }

    @Override
    public RT setConsistent(boolean consistent) {
        this.consistentRead = consistent;
        return (RT) this;
    }

    @Override
    public RT setExtra(Object obj) {
        this.extra = obj;
        return (RT) this;
    }

    @Override
    public RT setRequestHostName(String requestHostName) {
        this.requestHostName = requestHostName;
        return (RT) this;
    }

    @Override
    public RT setRequestId(Long requestId) {
        this.requestId = requestId;
        return (RT) this;
    }

    @Override
    public RT setSql(String sql) {
        this.sql = sql;
        return (RT) this;
    }

    @Override
    public RT setStreaming(boolean streaming) {
        this.streaming = streaming;
        return (RT) this;
    }

    @Override
    public RT setSubRequestId(Long subRequestId) {
        this.subRequestId = subRequestId;
        return (RT) this;
    }

    /**
     * 表明一个建议的用于执行该节点的线程id
     */
    @Override
    public RT setThread(Integer i) {
        this.thread = i;
        return (RT) this;
    }

    @Override
    public RT setUseBIO(boolean useBIO) {
        this.useBIO = useBIO;
        return (RT) this;
    }

    public void ensureCapacity(Collection collection, int minCapacity) {
        while (collection.size() <= minCapacity) {
            collection.add(null);
        }
    }

    @Override
    public boolean lazyLoad() {
        return this.lazyLoad;
    }

    @Override
    public void setLazyLoad(boolean lazyLoad) {
        this.lazyLoad = lazyLoad;
    }

    @Override
    public boolean isExistSequenceVal() {
        return this.existSequenceVal;
    }

    @Override
    public void setExistSequenceVal(boolean existSequenceVal) {
        this.existSequenceVal = existSequenceVal;
    }

    @Override
    public Long getLastSequenceVal() {
        return lastSequenceVal;
    }

    @Override
    public void setLastSequenceVal(Long lastSequenceVal) {
        this.lastSequenceVal = lastSequenceVal;
    }

    @Override
    public ExplainMode getExplainMode() {
        return explainMode;
    }

    @Override
    public void setExplainMode(ExplainMode explainMode) {
        this.explainMode = explainMode;
    }

    @Override
    public boolean isExplain() {
        return explainMode != null;
    }

    @Override
    public String toString() {
        return toStringWithInden(0, ExplainMode.DETAIL);
    }
}
