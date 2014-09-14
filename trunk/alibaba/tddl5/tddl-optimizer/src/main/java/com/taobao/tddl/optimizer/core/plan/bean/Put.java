package com.taobao.tddl.optimizer.core.plan.bean;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.dml.IDelete;
import com.taobao.tddl.optimizer.core.plan.dml.IInsert;
import com.taobao.tddl.optimizer.core.plan.dml.IReplace;
import com.taobao.tddl.optimizer.core.plan.dml.IUpdate;
import com.taobao.tddl.optimizer.utils.OptimizerToString;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public abstract class Put<RT extends IPut> extends DataNodeExecutor<RT> implements IPut<RT> {

    protected IQueryTree         queryTree;
    protected List<ISelectable>  columns;
    protected List<Object>       values;
    protected PUT_TYPE           putType;
    protected String             indexName;                      // 逻辑索引信息
    protected String             tableName;                      // 真实表名

    protected boolean            lowPriority  = false;
    protected boolean            highPriority = false;
    protected boolean            delayed      = false;
    protected boolean            quick        = false;
    protected boolean            ignore       = false;

    protected List<List<Object>> multiValues;
    protected boolean            isMultiValues;
    protected List<Integer>      batchIndexs  = new ArrayList(0);

    public Put(){
        putType = PUT_TYPE.REPLACE;
    }

    @Override
    public IQueryTree getQueryTree() {
        return queryTree;
    }

    @Override
    public RT setQueryTree(IQueryTree queryTree) {
        this.queryTree = queryTree;
        return (RT) this;
    }

    @Override
    public RT setUpdateColumns(List<ISelectable> columns) {
        this.columns = columns;
        return (RT) this;
    }

    @Override
    public List<ISelectable> getUpdateColumns() {
        return columns;
    }

    @Override
    public RT setTableName(String tableName) {
        this.tableName = tableName;
        return (RT) this;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public RT setUpdateValues(List<Object> values) {
        this.values = values;
        return (RT) this;
    }

    @Override
    public List<Object> getUpdateValues() {
        return values;
    }

    @Override
    public com.taobao.tddl.optimizer.core.plan.IPut.PUT_TYPE getPutType() {
        return putType;
    }

    @Override
    public RT setIndexName(String indexName) {
        this.indexName = indexName;
        return (RT) this;
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public RT setIgnore(boolean ignore) {
        this.ignore = ignore;
        return (RT) this;
    }

    @Override
    public boolean isIgnore() {
        return ignore;
    }

    @Override
    public List<List<Object>> getMultiValues() {
        return multiValues;
    }

    @Override
    public RT setMultiValues(List<List<Object>> multiValues) {
        this.multiValues = multiValues;
        return (RT) this;
    }

    @Override
    public boolean isMultiValues() {
        return isMultiValues;
    }

    @Override
    public RT setMultiValues(boolean isMultiValues) {
        this.isMultiValues = isMultiValues;
        return (RT) this;
    }

    @Override
    public int getMultiValuesSize() {
        if (this.isMultiValues) {
            return this.multiValues.size();
        } else {
            return 1;
        }
    }

    @Override
    public List<Object> getValues(int index) {
        if (this.isMultiValues) {
            return this.multiValues.get(index);
        }

        return this.values;
    }

    @Override
    public void accept(PlanVisitor visitor) {
        if (this instanceof IInsert) {
            visitor.visit((IInsert) this);
        } else if (this instanceof IDelete) {
            visitor.visit((IDelete) this);
        } else if (this instanceof IUpdate) {
            visitor.visit((IUpdate) this);
        } else if (this instanceof IReplace) {
            visitor.visit((IReplace) this);
        }
    }

    public void copySelfTo(IPut put) {
        put.setQueryTree((IQueryTree) this.queryTree.copy());
        put.setUpdateColumns(OptimizerUtils.copySelectables(columns));
        put.setUpdateValues(OptimizerUtils.copyValues(values));
        put.setIndexName(this.indexName);
        put.setTableName(this.tableName);
        put.setLowPriority(lowPriority);
        put.setHighPriority(highPriority);
        put.setDelayed(delayed);
        put.setQuick(quick);
        put.setIgnore(ignore);
        put.setMultiValues(isMultiValues);
        if (multiValues != null) {
            List<List<Object>> newMultiValues = new ArrayList<List<Object>>();
            for (List<Object> values : multiValues) {
                newMultiValues.add(OptimizerUtils.copyValues(values));
            }

            put.setMultiValues(newMultiValues);
        }

        put.setBatchIndexs(new ArrayList(this.batchIndexs));
    }

    @Override
    public String toStringWithInden(int inden, ExplainMode mode) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        appendln(sb, tabTittle + "Put:" + this.getPutType());
        appendField(sb, "tableName", this.getTableName(), tabContent);
        appendField(sb, "indexName", this.getIndexName(), tabContent);
        appendField(sb, "columns", this.getUpdateColumns(), tabContent);
        if (this.isMultiValues()) {
            appendField(sb, "multiValues", this.getMultiValues(), tabContent);
        } else {
            appendField(sb, "values", this.getUpdateValues(), tabContent);
        }

        if (mode.isDetail()) {
            // appendField(sb, "requestId", this.getRequestId(), tabContent);
            // appendField(sb, "subRequestId", this.getSubRequestId(),
            // tabContent);
            // appendField(sb, "thread", this.getThread(), tabContent);
            // appendField(sb, "hostname", this.getRequestHostName(),
            // tabContent);
        }
        if (mode.isSimple()) {
            appendField(sb, "executeOn", this.getDataNode(), tabContent);
            appendField(sb, "batchIndexs", this.getBatchIndexs(), tabContent);
        }

        if (this.getQueryTree() != null) {
            appendln(sb, tabContent + "query:");
            sb.append(this.getQueryTree().toStringWithInden(inden + 2, mode));
        }
        return sb.toString();
    }

    @Override
    public List<Integer> getBatchIndexs() {
        return this.batchIndexs;
    }

    @Override
    public RT setBatchIndexs(List<Integer> batchIndexs) {
        this.batchIndexs = batchIndexs;
        return (RT) this;
    }

    @Override
    public boolean isQuick() {
        return this.quick;
    }

    @Override
    public void setQuick(boolean quick) {
        this.quick = quick;

    }

    @Override
    public boolean isLowPriority() {
        return lowPriority;
    }

    @Override
    public void setLowPriority(boolean lowPriority) {
        this.lowPriority = lowPriority;
    }

    @Override
    public boolean isHighPriority() {
        return highPriority;
    }

    @Override
    public void setHighPriority(boolean highPriority) {
        this.highPriority = highPriority;
    }

    @Override
    public boolean isDelayed() {
        return delayed;
    }

    @Override
    public void setDelayed(boolean delayed) {
        this.delayed = delayed;
    }
}
