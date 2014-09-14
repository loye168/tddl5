package com.taobao.tddl.optimizer.core.plan.query;

import java.util.List;

import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;

/**
 * @since 5.0.0
 */
public interface IMerge extends IQueryTree<IQueryTree> {

    public List<IDataNodeExecutor> getSubNodes();

    /**
     * 获取一个subNode
     */
    public IDataNodeExecutor getSubNode();

    public IMerge setSubNodes(List<IDataNodeExecutor> subNode);

    public IMerge addSubNode(IDataNodeExecutor subNode);

    /**
     * Merge可以根据中间结果得知具体在哪个节点上进行查询 所以Merge的分库操作可以放到执行器中进行
     * 
     * @return true 表示已经经过sharding 。 false表示未经处理
     */
    public Boolean isSharded();

    public IMerge setSharded(boolean sharded);

    public Boolean isUnion();

    public IMerge setUnion(boolean isUnion);

    /**
     * 是否为group by分库键
     * 
     * @return
     */
    public boolean isGroupByShardColumns();

    public IMerge setGroupByShardColumns(boolean groupByShardColumns);

    /**
     * 是否为group by分库键
     * 
     * @return
     */
    public boolean isDistinctByShardColumns();

    public IMerge setDistinctByShardColumns(boolean distinctByShardColumns);

    /**
     * 是否为广播表多写
     */
    public boolean isDmlByBroadcast();

    public IMerge setDmlByBroadcast(boolean isDmlByBroadcast);
}
