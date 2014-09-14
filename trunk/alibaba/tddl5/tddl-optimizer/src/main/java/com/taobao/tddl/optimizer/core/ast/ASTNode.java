package com.taobao.tddl.optimizer.core.ast;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.ast.delegate.ShareDelegate;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

/**
 * 可优化的语法树
 * 
 * @since 5.0.0
 */
public abstract class ASTNode<RT extends ASTNode> implements Comparable {

    private List<String> dataNodes        = new ArrayList<String>(); // 数据处理节点,比如groupName
    private List<Object> extras           = new ArrayList<Object>(); // 比如唯一标识，joinMergeJoin中使用
    protected boolean    broadcast        = false;                  // 是否为广播表
    protected String     sql;
    protected boolean    existSequenceVal = false;                  // 是否存在sequence

    public ASTNode(){

    }

    /**
     * <pre>
     * 1. 结合table meta信息构建结构树中完整的column字段
     * 2. 处理join/merge的下推处理
     * </pre>
     */
    public abstract void build();

    /**
     * 需要预先执行build.构造执行计划
     */
    @ShareDelegate
    public IDataNodeExecutor toDataNodeExecutor() {
        return toDataNodeExecutor(0);
    }

    /**
     * 需要预先执行build.构造执行计划
     */
    public abstract IDataNodeExecutor toDataNodeExecutor(int shareIndex);

    /**
     * 处理bind val
     */
    public abstract void assignment(Parameters parameterSettings);

    public abstract boolean isNeedBuild();

    @ShareDelegate
    public String getDataNode() {
        return getDataNode(0);
    }

    @ShareDelegate
    public RT executeOn(String dataNode) {
        return executeOn(dataNode, 0);
    }

    public String getDataNode(int shareIndex) {
        ensureCapacity(dataNodes, shareIndex);
        return dataNodes.get(shareIndex);
    }

    public RT executeOn(String dataNode, int shareIndex) {
        ensureCapacity(dataNodes, shareIndex);
        this.dataNodes.set(shareIndex, dataNode);
        return (RT) this;
    }

    @ShareDelegate
    public Object getExtra() {
        return getExtra(0);
    }

    @ShareDelegate
    public RT setExtra(Object obj) {
        return setExtra(obj, 0);
    }

    public Object getExtra(int shareIndex) {
        ensureCapacity(extras, shareIndex);
        return this.extras.get(shareIndex);
    }

    public RT setExtra(Object obj, int shareIndex) {
        ensureCapacity(extras, shareIndex);
        this.extras.set(shareIndex, obj);
        return (RT) this;
    }

    public String getSql() {
        return this.sql;
    }

    public RT setSql(String sql) {
        this.sql = sql;
        return (RT) this;
    }

    public boolean isBroadcast() {
        return broadcast;
    }

    public void setBroadcast(boolean broadcast) {
        this.broadcast = broadcast;
    }

    public int compareTo(Object arg) {
        // 主要是将自己包装为Comparable对象，可以和Number/string类型具有相同的父类，构建嵌套的查询树
        throw new NotSupportException();
    }

    @ShareDelegate
    public String toString() {
        return toString(0, 0);
    }

    @ShareDelegate
    public String toString(int inden) {
        return toString(inden, 0);
    }

    public abstract String toString(int inden, int shareIndex);

    public boolean isShareNode() {
        return dataNodes.size() > 1;
    }

    public int getShareSize() {
        return dataNodes.size();
    }

    public void ensureCapacity(Collection collection, int minCapacity) {
        while (collection.size() <= minCapacity) {
            collection.add(null);
        }
    }

    public boolean isExistSequenceVal() {
        return existSequenceVal;
    }

    public void setExistSequenceVal(boolean existSequenceVal) {
        this.existSequenceVal = existSequenceVal;
    }

    /**
     * 获取对应的subquery filter,不包括correlate subquery
     */
    public abstract IFunction getNextSubqueryOnFilter();

    // ----------------- 复制 ----------------

    /**
     * 复制当前节点和子节点，属性信息不做递归复制
     */
    public abstract RT copy();

    /**
     * 只复制当前节点，不复制子节点
     */
    public abstract RT copySelf();

    /**
     * 复制当前节点和子节点，属性信息进行递归复制
     */
    public abstract RT deepCopy();
}
