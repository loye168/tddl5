package com.taobao.tddl.optimizer.core.ast.dml;

import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.dml.IReplace;
import com.taobao.tddl.optimizer.rule.OptimizerRule;

public class PutNode extends DMLNode<PutNode> {

    public PutNode(TableNode table){
        super(table);
    }

    @Override
    public IPut toDataNodeExecutor(int shareIndex) {
        IReplace put = ASTNodeFactory.getInstance().createReplace();
        if (this.getNode().getActualTableName() != null) {
            put.setTableName(this.getNode().getActualTableName());
        } else if (this.getNode() instanceof KVIndexNode) {
            put.setTableName(((KVIndexNode) this.getNode()).getIndexName());
        } else {
            put.setTableName(this.getNode().getTableName());
        }
        put.setIndexName(this.getNode().getIndexUsed().getName());
        put.setConsistent(true);
        put.setUpdateColumns(this.getColumns());
        put.setUpdateValues(this.getValues());
        put.executeOn(this.getNode().getDataNode());
        put.setBatchIndexs(this.getBatchIndexs());
        if (this.getSelectNode() != null) {
            put.setQueryTree((IQueryTree) this.getSelectNode().toDataNodeExecutor());
        }
        put.setIgnore(this.isIgnore());
        put.setQuick(this.isQuick());
        put.setLowPriority(this.lowPriority);
        put.setHighPriority(this.highPriority);
        put.setDelayed(this.isDelayed());
        put.setMultiValues(this.isMultiValues());
        put.setMultiValues(this.getMultiValues());
        put.setExistSequenceVal(this.isExistSequenceVal());
        return put;
    }

    @Override
    public PutNode deepCopy() {
        PutNode put = new PutNode(null);
        super.deepCopySelfTo(put);
        return put;
    }

    @Override
    public PutNode copy() {
        PutNode put = new PutNode(null);
        super.copySelfTo(put);
        return put;
    }

    public boolean processAutoIncrement() {
        if (!super.processAutoIncrement()) {
            return false;
        }

        // 针对分库分表进行处理,广播表进行处理
        String tableName = getTableMeta().getTableName();
        OptimizerRule rule = OptimizerContext.getContext().getRule();
        return !rule.isTableInSingleDb(tableName) || rule.isBroadCast(tableName);
    }

}
