package com.taobao.tddl.optimizer.core.ast.dml;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.dml.IDelete;

public class DeleteNode extends DMLNode<DeleteNode> {

    public DeleteNode(TableNode table){
        super(table);
    }

    @Override
    public IPut toDataNodeExecutor(int shareIndex) {
        IDelete delete = ASTNodeFactory.getInstance().createDelete();
        delete.setConsistent(true);
        delete.executeOn(this.getNode().getDataNode());
        delete.setQueryTree((IQueryTree) this.getNode().toDataNodeExecutor());
        if (this.getNode().getActualTableName() != null) {
            delete.setTableName(this.getNode().getActualTableName());
        } else if (this.getNode() instanceof KVIndexNode) {
            delete.setTableName(((KVIndexNode) this.getNode()).getIndexName());
        } else {
            delete.setTableName(this.getNode().getTableName());
        }
        delete.setIndexName(this.getNode().getIndexUsed().getName());
        delete.setBatchIndexs(this.getBatchIndexs());

        delete.setIgnore(this.isIgnore());
        delete.setQuick(this.isQuick());
        delete.setLowPriority(this.lowPriority);
        delete.setHighPriority(this.highPriority);
        delete.setDelayed(this.isDelayed());
        delete.setMultiValues(this.isMultiValues());
        delete.setMultiValues(this.getMultiValues());
        delete.setExistSequenceVal(this.isExistSequenceVal());
        return delete;
    }

    @Override
    public DeleteNode deepCopy() {
        DeleteNode delete = new DeleteNode(null);
        super.deepCopySelfTo(delete);
        return delete;
    }

    @Override
    public DeleteNode copy() {
        DeleteNode delete = new DeleteNode(null);
        super.copySelfTo(delete);
        return delete;
    }

    @Override
    public boolean processAutoIncrement() {
        // delete不开启
        return false;
    }

}
