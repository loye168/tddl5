package com.taobao.tddl.optimizer.core.plan;

import java.util.List;

import com.taobao.tddl.optimizer.core.expression.ISelectable;

public interface IPut<RT extends IPut> extends IDataNodeExecutor<RT> {

    public enum PUT_TYPE {
        REPLACE, INSERT, DELETE, UPDATE;
    }

    /**
     * depend query command
     * 
     * @return
     */
    IQueryTree getQueryTree();

    /**
     * @param queryCommon
     */
    RT setQueryTree(IQueryTree queryTree);

    /**
     * set a = 1 ,b = 2 , c = 3 那么这个应该是 [‘a‘,‘b‘,‘c‘]
     * 
     * @param columns
     */
    RT setUpdateColumns(List<ISelectable> columns);

    List<ISelectable> getUpdateColumns();

    /**
     * IdxName
     * 
     * @param indexName
     */
    RT setTableName(String indexName);

    String getTableName();

    RT setIndexName(String indexName);

    String getIndexName();

    /**
     * set a = 1 ,b = 2 , c = 3 那么这个应该是 [1，2，3]
     * 
     * @param columns
     */
    RT setUpdateValues(List<Object> values);

    List<Object> getUpdateValues();

    PUT_TYPE getPutType();

    RT setIgnore(boolean ignore);

    boolean isIgnore();

    /**
     * 用于多值insert
     * 
     * @return
     */
    public List<List<Object>> getMultiValues();

    public RT setMultiValues(List<List<Object>> multiValues);

    public boolean isMultiValues();

    public RT setMultiValues(boolean isMutiValues);

    public int getMultiValuesSize();

    public List<Object> getValues(int index);

    /**
     * 这个节点上执行哪些batch
     * 
     * @return
     */
    public List<Integer> getBatchIndexs();

    public RT setBatchIndexs(List<Integer> batchIndexs);

    boolean isDelayed();

    void setHighPriority(boolean highPriority);

    void setLowPriority(boolean lowPriority);

    boolean isLowPriority();

    boolean isHighPriority();

    void setQuick(boolean quick);

    boolean isQuick();

    void setDelayed(boolean delayed);

}
