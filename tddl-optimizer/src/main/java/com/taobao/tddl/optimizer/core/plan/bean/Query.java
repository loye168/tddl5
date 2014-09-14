package com.taobao.tddl.optimizer.core.plan.bean;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.printFilterString;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.OptimizerToString;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public class Query extends QueryTree implements IQuery {

    protected IFilter    keyFilter;

    protected String     indexName;
    protected String     tableName;
    protected IQueryTree subQuery;

    @Override
    public IFilter getKeyFilter() {
        return keyFilter;
    }

    @Override
    public IQuery setKeyFilter(IFilter keyFilter) {
        this.keyFilter = keyFilter;
        return this;
    }

    @Override
    public IQuery setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public String getIndexName() {
        return this.indexName;
    }

    @Override
    public IQuery setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    @Override
    public IQuery setSubQuery(IQueryTree subQuery) {
        this.subQuery = subQuery;
        return this;
    }

    @Override
    public IQueryTree getSubQuery() {
        return subQuery;
    }

    @Override
    public IQuery copy() {
        IQuery query = ASTNodeFactory.getInstance().createQuery();
        copySelfTo((QueryTree) query);

        if (this.getSubQuery() != null) {
            query.setSubQuery((IQueryTree) this.getSubQuery().copy());
        }
        query.setTableName(this.getTableName());
        query.setIndexName(this.getIndexName());
        query.setKeyFilter(OptimizerUtils.copyFilter(this.getKeyFilter()));
        return query;
    }

    @Override
    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toStringWithInden(int inden, ExplainMode mode) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();

        if (this.getTableName() != null) {
            if (this.getAlias() != null) {
                appendln(sb, tabTittle + "Query from " + this.getIndexName() + " as " + this.getAlias());
            } else {
                appendln(sb, tabTittle + "Query from " + this.getIndexName());
            }
        } else {
            if (this.getAlias() != null) {
                appendln(sb, tabTittle + "Query" + " as " + this.getAlias());
            } else {
                appendln(sb, tabTittle + "Query");
            }
        }
        appendField(sb, "keyFilter", printFilterString(this.getKeyFilter()), tabContent);
        appendField(sb, "resultFilter", printFilterString(this.getValueFilter()), tabContent);
        appendField(sb, "having", printFilterString(this.getHavingFilter()), tabContent);
        if (!(this.getLimitFrom() != null && this.getLimitFrom().equals(-1L) && this.getLimitTo() != null && this.getLimitTo()
            .equals(-1L))) {
            appendField(sb, "limitFrom", this.getLimitFrom(), tabContent);
            appendField(sb, "limitTo", this.getLimitTo(), tabContent);
        }

        if (this.isSubQuery() != null && this.isSubQuery()) {
            appendField(sb, "isSubQuery", this.isSubQuery(), tabContent);
        }
        appendField(sb, "orderBy", this.getOrderBys(), tabContent);
        appendField(sb, "queryConcurrency", this.getQueryConcurrency(), tabContent);
        if (this.getLockMode() != LOCK_MODE.UNDEF) {
            appendField(sb, "lockModel", this.getLockMode(), tabContent);
        }
        if (mode.isDetail()) {
            appendField(sb, "columns", this.getColumns(), tabContent);
        }
        appendField(sb, "groupBys", this.getGroupBys(), tabContent);
        appendField(sb, "sql", this.getSql(), tabContent);
        if (this.getSubqueryOnFilterId() > 0) {
            appendField(sb, "subqueryOnFilterId", this.getSubqueryOnFilterId(), tabContent);
        }

        if (mode.isSimple()) {
            appendField(sb, "tableName", this.getTableName(), tabContent);
            appendField(sb, "executeOn", this.getDataNode(), tabContent);
        }

        if (mode.isDetail()) {
            // appendField(sb, "requestID",
            // this.getRequestID(), tabContent);
            // appendField(sb, "subRequestID",
            // this.getSubRequestID(), tabContent);
            // if (this.getThread() != null)
            // appendField(sb, "thread",
            // this.getThread(), tabContent);
        }

        if (this.getSubQuery() != null) {
            appendln(sb, tabContent + "from:");
            sb.append(this.getSubQuery().toStringWithInden(inden + 2, mode));
        }

        return sb.toString();
    }

}
