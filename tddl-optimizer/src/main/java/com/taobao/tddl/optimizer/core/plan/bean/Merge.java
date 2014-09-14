package com.taobao.tddl.optimizer.core.plan.bean;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.printFilterString;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.utils.OptimizerToString;

public class Merge extends QueryTree implements IMerge {

    protected List<IDataNodeExecutor> subNodes                 = new LinkedList<IDataNodeExecutor>();
    protected Boolean                 isSharded                = true;
    protected Boolean                 isUnion                  = false;
    protected Boolean                 isGroupByShardColumns    = false;
    protected Boolean                 isDistinctByShardColumns = false;
    protected Boolean                 isDmlByBroadcast         = false;

    public IQueryTree copy() {
        IMerge merge = ASTNodeFactory.getInstance().createMerge();
        this.copySelfTo((QueryTree) merge);
        for (IDataNodeExecutor dne : this.getSubNodes()) {
            merge.addSubNode(dne.copy());
        }
        merge.setSharded(this.isSharded);
        merge.setUnion(this.isUnion);
        merge.setGroupByShardColumns(this.isGroupByShardColumns);
        merge.setDistinctByShardColumns(this.isDistinctByShardColumns);
        return merge;
    }

    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    public List<IDataNodeExecutor> getSubNodes() {
        return subNodes;
    }

    public IDataNodeExecutor getSubNode() {
        if (this.subNodes.isEmpty()) {
            return null;
        }

        return subNodes.get(0);
    }

    public IMerge setSubNodes(List<IDataNodeExecutor> subNodes) {
        this.subNodes = subNodes;
        return this;
    }

    public IMerge addSubNode(IDataNodeExecutor subNode) {
        subNodes.add(subNode);
        return this;
    }

    public Boolean isSharded() {
        return isSharded;
    }

    public IMerge setSharded(boolean isSharded) {
        this.isSharded = isSharded;
        return this;
    }

    public Boolean isUnion() {
        return isUnion;
    }

    public IMerge setUnion(boolean isUnion) {
        this.isUnion = isUnion;
        return this;
    }

    @Override
    public boolean isGroupByShardColumns() {
        return isGroupByShardColumns;
    }

    @Override
    public IMerge setGroupByShardColumns(boolean isGroupByShardColumns) {
        this.isGroupByShardColumns = isGroupByShardColumns;
        return this;
    }

    @Override
    public boolean isDistinctByShardColumns() {
        return isDistinctByShardColumns;
    }

    @Override
    public IMerge setDistinctByShardColumns(boolean distinctByShardColumns) {
        this.isDistinctByShardColumns = distinctByShardColumns;
        return this;
    }

    public boolean isDmlByBroadcast() {
        return isDmlByBroadcast;
    }

    public IMerge setDmlByBroadcast(boolean isDmlByBroadcast) {
        this.isDmlByBroadcast = isDmlByBroadcast;
        return this;
    }

    public String toStringWithInden(int inden, ExplainMode mode) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();

        if (this.getAlias() != null) {
            appendln(sb, tabTittle + (this.isUnion() ? "Union" : "Merge") + " as " + this.getAlias());
        } else {
            appendln(sb, tabTittle + (this.isUnion() ? "Union" : "Merge"));
        }

        appendField(sb, "valueFilter", printFilterString(this.getValueFilter()), tabContent);
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
        if (mode.isDetail()) {
            appendField(sb, "columns", this.getColumns(), tabContent);
        }
        appendField(sb, "groupBys", this.getGroupBys(), tabContent);
        if (this.getSubqueryOnFilterId() > 0) {
            appendField(sb, "subqueryOnFilterId", this.getSubqueryOnFilterId(), tabContent);
        }

        if (mode.isSimple()) {
            appendField(sb, "executeOn", this.getDataNode(), tabContent);
        }

        if (mode.isDetail()) {
            // if(this.getThread()!=null)
            // appendField(sb, "thread",
            // this.getThread(), tabContent);
            // appendField(sb, "requestID",
            // this.getRequestID(), tabContent);
            // appendField(sb, "subRequestID",
            // this.getSubRequestID(), tabContent);
        }
        // 先做一次subquery合并
        int mergeSize = 0;
        List<String> executeNodes = new ArrayList<String>();
        if (!mode.isDetail() && this.getSubNodes().size() > 1) {
            String lastQuery = null;
            for (IDataNodeExecutor s : this.getSubNodes()) {
                String query = s.toStringWithInden(inden + 2, ExplainMode.LOGIC);
                if (lastQuery == null) {
                    lastQuery = query;
                    executeNodes.add(s.getDataNode());
                } else if (lastQuery.equals(query)) {
                    executeNodes.add(s.getDataNode());
                } else if (!lastQuery.equals(query)) {
                    // 不是相同的逻辑节点
                    break;
                }
                mergeSize++;
            }
        }

        if (mergeSize > 0) {
            String mergeStr = "subQueries merge[" + mergeSize + "]";
            if (mode.isSimple()) {
                mergeStr += " and executeOn" + executeNodes;
            }
            appendln(sb, tabContent + mergeStr);
            // 只显示第一个
            sb.append(this.getSubNode().toStringWithInden(inden + 2, mode));
        } else {
            for (IDataNodeExecutor s : this.getSubNodes()) {
                sb.append(s.toStringWithInden(inden + 2, mode));
            }
        }
        return sb.toString();
    }
}
