package com.taobao.tddl.optimizer.core.plan.bean;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public abstract class QueryTree extends DataNodeExecutor<IQueryTree> implements IQueryTree<IQueryTree> {

    protected IFilter           valueFilter;
    protected IFilter           havingFilter;
    protected IFilter           subqueryFilter;
    protected List<IOrderBy>    orderBys             = Collections.emptyList();
    protected List<IOrderBy>    groupBys             = Collections.emptyList();
    protected Comparable        limitFrom;
    protected Comparable        limitTo;
    protected List<ISelectable> columns              = Collections.emptyList();
    protected String            alias;
    /**
     * 查询模式，并行？串行？
     */
    protected QUERY_CONCURRENCY queryConcurrency     = QUERY_CONCURRENCY.SEQUENTIAL;

    /**
     * 能否被合并成一条sql，默认可以
     */
    protected Boolean           canMerge             = true;

    /**
     * 是否显式使用临时表，默认不可以
     */
    protected Boolean           useTempTableExplicit = false;
    protected Boolean           isSubQuery           = false;
    protected boolean           isTopQuery           = false;
    /**
     * 是否为存在聚合信息，比如出现limit/group by/count/max等，此节点就会被标记为true，不允许进行join merge
     * join的展开优化
     */
    protected boolean           isExistAggregate     = false;

    /**
     * 非column=column的join列
     */
    protected IFilter           otherJoinOnFilter;
    protected Long              subqueryOnFilterId   = 0L;
    protected boolean           isCorrelatedSubquery = false;
    protected LOCK_MODE         lockModel            = LOCK_MODE.UNDEF;

    @Override
    public IFilter getValueFilter() {
        return valueFilter;
    }

    @Override
    public IQueryTree setValueFilter(IFilter valueFilter) {
        this.valueFilter = valueFilter;
        return this;
    }

    @Override
    public IFilter getSubqueryFilter() {
        return subqueryFilter;
    }

    @Override
    public IQueryTree setSubqueryFilter(IFilter subqueryFilter) {
        this.subqueryFilter = subqueryFilter;
        return this;
    }

    @Override
    public List<ISelectable> getColumns() {
        return columns;
    }

    @Override
    public IQueryTree setColumns(List<ISelectable> columns) {
        this.columns = columns;
        return this;
    }

    @Override
    public IQueryTree setColumns(ISelectable... columns) {
        return setColumns(Arrays.asList(columns));
    }

    @Override
    public List<IOrderBy> getOrderBys() {
        return orderBys;
    }

    @Override
    public IQueryTree setOrderBys(List<IOrderBy> orderBys) {
        this.orderBys = orderBys;
        return this;
    }

    @Override
    public Comparable getLimitFrom() {
        return limitFrom;
    }

    @Override
    public IQueryTree setLimitFrom(Comparable limitFrom) {
        this.limitFrom = limitFrom;
        return this;
    }

    @Override
    public Comparable getLimitTo() {
        return limitTo;
    }

    @Override
    public IQueryTree setLimitTo(Comparable limitTo) {
        this.limitTo = limitTo;
        return this;
    }

    @Override
    public List<IOrderBy> getGroupBys() {
        return groupBys;
    }

    @Override
    public IQueryTree setGroupBys(List<IOrderBy> groupBys) {
        this.groupBys = groupBys;
        return this;
    }

    @Override
    public IQueryTree setAlias(String alias) {
        this.alias = alias;
        return this;
    }

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public IQueryTree setCanMerge(Boolean canMerge) {
        this.canMerge = canMerge;
        return this;
    }

    @Override
    public Boolean canMerge() {
        return canMerge;
    }

    @Override
    public IQueryTree setUseTempTableExplicit(Boolean isUseTempTable) {
        this.useTempTableExplicit = isUseTempTable;
        return this;
    }

    @Override
    public Boolean isUseTempTableExplicit() {
        return useTempTableExplicit;
    }

    @Override
    public Boolean isSubQuery() {
        return isSubQuery;
    }

    @Override
    public IQueryTree setIsSubQuery(Boolean isSubQuery) {
        this.isSubQuery = isSubQuery;
        return this;
    }

    @Override
    public IFilter getHavingFilter() {
        return havingFilter;
    }

    @Override
    public IQueryTree having(IFilter having) {
        this.havingFilter = having;
        return this;
    }

    @Override
    public boolean isTopQuery() {
        return isTopQuery;
    }

    @Override
    public IQueryTree setTopQuery(boolean topQuery) {
        this.isTopQuery = topQuery;
        return this;
    }

    @Override
    public IQueryTree setQueryConcurrency(QUERY_CONCURRENCY queryConcurrency) {
        this.queryConcurrency = queryConcurrency;
        return this;
    }

    @Override
    public QUERY_CONCURRENCY getQueryConcurrency() {
        return queryConcurrency;
    }

    @Override
    public boolean isExistAggregate() {
        return isExistAggregate;
    }

    @Override
    public IQueryTree setExistAggregate(boolean isExistAggregate) {
        this.isExistAggregate = isExistAggregate;
        return this;
    }

    public IFilter getOtherJoinOnFilter() {
        return otherJoinOnFilter;
    }

    public IQueryTree setOtherJoinOnFilter(IFilter otherJoinOnFilter) {
        this.otherJoinOnFilter = otherJoinOnFilter;
        return this;
    }

    public Long getSubqueryOnFilterId() {
        return subqueryOnFilterId;
    }

    public IQueryTree setSubqueryOnFilterId(Long subqueryOnFilterId) {
        this.subqueryOnFilterId = subqueryOnFilterId;
        return this;
    }

    @Override
    public boolean isCorrelatedSubquery() {
        return isCorrelatedSubquery;
    }

    @Override
    public IQueryTree setCorrelatedSubquery(boolean isCorrelatedSubquery) {
        this.isCorrelatedSubquery = isCorrelatedSubquery;
        return this;
    }

    protected void copySelfTo(QueryTree o) {
        o.setRequestId(this.getRequestId());
        o.setSubRequestId(this.getSubRequestId());
        o.setRequestHostName(this.getRequestHostName());
        o.setConsistent(this.getConsistent());
        o.setQueryConcurrency(this.getQueryConcurrency());
        o.setAlias(this.getAlias());
        o.setCanMerge(this.canMerge());
        o.setUseTempTableExplicit(this.isUseTempTableExplicit());
        o.setThread(getThread());
        o.setStreaming(this.isStreaming());
        o.setSql(this.getSql());
        o.setIsSubQuery(this.isSubQuery());
        o.setTopQuery(this.isTopQuery());
        o.executeOn(this.getDataNode());
        o.setExistAggregate(this.isExistAggregate());
        o.setLazyLoad(this.lazyLoad());
        o.setColumns(OptimizerUtils.copySelectables((this.getColumns())));
        o.setGroupBys(OptimizerUtils.copyOrderBys(this.getGroupBys()));
        o.setOrderBys(OptimizerUtils.copyOrderBys(this.getOrderBys()));
        o.setValueFilter(OptimizerUtils.copyFilter(this.getValueFilter()));
        o.having(OptimizerUtils.copyFilter(this.getHavingFilter()));
        o.setOtherJoinOnFilter(OptimizerUtils.copyFilter(this.getOtherJoinOnFilter()));
        o.setLockMode(this.lockModel);
        if (this.getLimitFrom() instanceof IBindVal) {
            o.setLimitFrom(((IBindVal) this.getLimitFrom()).copy());
        } else {
            o.setLimitFrom(this.getLimitFrom());
        }

        if (this.getLimitTo() instanceof IBindVal) {
            o.setLimitTo(((IBindVal) this.getLimitTo()).copy());
        } else {
            o.setLimitTo(this.getLimitTo());
        }
    }

    @Override
    public LOCK_MODE getLockMode() {
        return lockModel;
    }

    @Override
    public QueryTree setLockMode(LOCK_MODE lockModel) {
        this.lockModel = lockModel;
        return this;
    }

}
