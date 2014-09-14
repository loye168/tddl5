package com.taobao.tddl.optimizer.core.ast.query;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.printFilterString;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.build.KVIndexNodeBuilder;
import com.taobao.tddl.optimizer.core.ast.build.QueryTreeNodeBuilder;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree.LOCK_MODE;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * @author Dreamond
 * @author jianghang 2013-11-8 下午2:33:51
 * @since 5.0.0
 */
public class KVIndexNode extends TableNode {

    private IndexMeta          index       = null;
    private String             kvIndexName = null;

    private KVIndexNodeBuilder builder;

    public KVIndexNode(){
        this(null);
    }

    public KVIndexNode(String kvIndexName){
        super();
        this.kvIndexName = kvIndexName;
        builder = new KVIndexNodeBuilder(this);
    }

    public KVIndexNode keyQuery(IFilter f) {
        this.setNeedBuild(true);
        this.keyFilter = f;
        return this;
    }

    public KVIndexNode keyQuery(String f) {
        this.setNeedBuild(true);
        this.keyFilter = FilterUtils.createFilter(f);
        return this;
    }

    public KVIndexNode valueQuery(IFilter f) {
        this.setNeedBuild(true);
        this.resultFilter = f;
        return this;
    }

    public KVIndexNode valueQuery(String f) {
        this.setNeedBuild(true);
        this.resultFilter = FilterUtils.createFilter(f);
        return this;
    }

    public QueryTreeNodeBuilder getBuilder() {
        return builder;
    }

    public IDataNodeExecutor toDataNodeExecutor(int shareIndex) {
        subquerytoDataNodeExecutor(shareIndex);
        IQuery query = ASTNodeFactory.getInstance().createQuery();
        query.setAlias(this.getAlias());
        query.setColumns(this.getColumnsSelected());
        query.setConsistent(this.getConsistent());
        query.setGroupBys(this.getGroupBys());
        query.setIndexName(this.getIndex() == null ? null : this.getIndex().getName());
        query.setKeyFilter(this.getKeyFilter());
        query.setValueFilter(this.getResultFilter());
        query.setLimitFrom(this.getLimitFrom());
        query.setLimitTo(this.getLimitTo());
        query.setLockMode(this.getLockMode());
        query.setOrderBys(this.getOrderBys());
        query.setSubQuery(null);
        query.setSql(this.getSql());
        query.setIsSubQuery(this.isSubQuery());
        query.having(this.getHavingFilter());
        query.setExistAggregate(this.isExistAggregate());
        query.setOtherJoinOnFilter(this.getOtherJoinOnFilter());
        query.setSubqueryFilter(this.getSubqueryFilter());
        query.executeOn(this.getDataNode(shareIndex));
        String tableName = null;
        if (this.getActualTableName() != null) {
            tableName = this.getActualTableName(shareIndex);
        } else if (this.getIndex() != null) {
            tableName = this.getIndex().getName();
        }
        query.setTableName(tableName);
        query.setSubqueryOnFilterId(this.getSubqueryOnFilterId());
        query.setCorrelatedSubquery(this.isCorrelatedSubquery());
        query.setExistSequenceVal(this.isExistSequenceVal());
        return query;
    }

    public void build() {
        if (this.isNeedBuild()) {
            this.builder.build();
        }

        setNeedBuild(false);
    }

    // ================ setter / getter ===============

    public String getTableName() {
        return this.getIndex().getTableName();
    }

    public String getName() {
        if (this.getAlias() != null) {
            return this.getAlias();
        }

        return this.getIndexName();
    }

    public IndexMeta getIndexUsed() {
        return this.getIndex();
    }

    public String getIndexName() {
        return this.getKvIndexName();
    }

    public IndexMeta getIndex() {
        return index;
    }

    public void setIndex(IndexMeta index) {
        this.index = index;
    }

    public String getKvIndexName() {
        return kvIndexName;
    }

    public void setKvIndexName(String kvIndexName) {
        this.kvIndexName = kvIndexName;
    }

    public KVIndexNode copy() {
        KVIndexNode newTableNode = new KVIndexNode(this.getIndexName());
        this.copySelfTo(newTableNode);
        return newTableNode;
    }

    protected void copySelfTo(QueryTreeNode to) {
        super.copySelfTo(to);
        KVIndexNode toTable = (KVIndexNode) to;
        toTable.index = index;
        toTable.kvIndexName = kvIndexName;
        toTable.setTableMeta(this.getTableMeta());
    }

    public KVIndexNode deepCopy() {
        KVIndexNode newTableNode = new KVIndexNode(this.getIndexName());
        this.deepCopySelfTo(newTableNode);
        return newTableNode;
    }

    protected void deepCopySelfTo(QueryTreeNode to) {
        super.deepCopySelfTo(to);
        KVIndexNode toTable = (KVIndexNode) to;
        toTable.index = index;
        toTable.kvIndexName = kvIndexName;
        toTable.setTableMeta(this.getTableMeta());
    }

    public void assignment(Parameters parameterSettings) {
        super.assignment(parameterSettings);
    }

    public List<IOrderBy> getImplicitOrderBys() {
        List<IOrderBy> orderByCombineWithGroupBy = getOrderByCombineWithGroupBy();
        if (orderByCombineWithGroupBy != null) {
            return orderByCombineWithGroupBy;
        }

        List<IOrderBy> implicitOrdersCandidate = OptimizerUtils.getOrderBy(index);
        List<IOrderBy> implicitOrders = new ArrayList();
        for (int i = 0; i < implicitOrdersCandidate.size(); i++) {
            implicitOrdersCandidate.get(i).setTableName(this.getIndexName());
            if (this.getColumnsSelected().contains(implicitOrdersCandidate.get(i).getColumn())) {
                implicitOrders.add(implicitOrdersCandidate.get(i));
            } else {
                break;
            }
        }

        return implicitOrders;
    }

    public String getSchemaName() {
        return this.getIndexName();
    }

    public String toString(int inden, int shareIndex) {
        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();

        if (this.getAlias() != null) {
            appendln(sb, tabTittle + "Query from " + this.getIndexName() + " as " + this.getAlias());
        } else {
            appendln(sb, tabTittle + "Query from " + this.getIndexName());
        }
        appendField(sb, "actualTableName", this.getActualTableName(shareIndex), tabContent);
        appendField(sb, "keyFilter", printFilterString(this.getKeyFilter()), tabContent);
        appendField(sb, "resultFilter", printFilterString(this.getResultFilter()), tabContent);
        appendField(sb, "whereFilter", printFilterString(this.getWhereFilter()), tabContent);
        appendField(sb, "subqueryFilter", printFilterString(this.getSubqueryFilter(), inden + 2), tabContent);
        appendField(sb, "having", printFilterString(this.getHavingFilter()), tabContent);
        if (!(this.getLimitFrom() != null && this.getLimitFrom().equals(0L) && this.getLimitTo() != null && this.getLimitTo()
            .equals(0L))) {
            appendField(sb, "limitFrom", this.getLimitFrom(), tabContent);
            appendField(sb, "limitTo", this.getLimitTo(), tabContent);
        }

        if (this.isSubQuery()) {
            appendField(sb, "isSubQuery", this.isSubQuery(), tabContent);
        }
        appendField(sb, "orderBy", this.getOrderBys(), tabContent);
        appendField(sb, "queryConcurrency", this.getQueryConcurrency(), tabContent);
        if (this.getLockMode() != LOCK_MODE.UNDEF) {
            appendField(sb, "lockModel", this.getLockMode(), tabContent);
        }
        appendField(sb, "columns", this.getColumnsSelected(), tabContent);
        appendField(sb, "groupBys", this.getGroupBys(), tabContent);
        appendField(sb, "sql", this.getSql(), tabContent);
        if (this.getSubqueryOnFilterId() > 0) {
            appendField(sb, "subqueryOnFilterId", this.getSubqueryOnFilterId(), tabContent);
        }
        appendField(sb, "executeOn", this.getDataNode(shareIndex), tabContent);
        return sb.toString();

    }
}
