package com.taobao.tddl.optimizer.core.ast.dml;

import java.util.List;

import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.dml.IUpdate;

public class UpdateNode extends DMLNode<UpdateNode> {

    public UpdateNode(TableNode table){
        super(table);
    }

    public UpdateNode setUpdateColumns(List<ISelectable> columns) {
        this.columns = columns;
        return this;
    }

    public List<ISelectable> getUpdateColumns() {
        return this.columns;
    }

    public UpdateNode setUpdateValues(List<Object> values) {
        this.values = values;
        return this;
    }

    public List<Object> getUpdateValues() {
        return this.values;
    }

    @Override
    public IPut toDataNodeExecutor(int shareIndex) {
        IUpdate update = ASTNodeFactory.getInstance().createUpdate();
        for (ISelectable updateColumn : this.getColumns()) {
            List<String> partiionColumns = OptimizerContext.getContext()
                .getRule()
                .getSharedColumns(this.getNode().getTableMeta().getTableName());

            if (partiionColumns.contains(updateColumn.getColumnName())) {
                throw new IllegalArgumentException("column :" + updateColumn.getColumnName() + " 是分库键，不允许修改");
            }

            if (this.getNode().getTableMeta().getPrimaryKeyMap().containsKey(updateColumn.getColumnName())) {
                throw new IllegalArgumentException("column :" + updateColumn.getColumnName() + " 是主键，不允许修改");
            }
        }

        update.setConsistent(true);
        update.executeOn(this.getNode().getDataNode());
        update.setQueryTree((IQueryTree) this.getNode().toDataNodeExecutor());
        update.setUpdateColumns(this.getUpdateColumns());
        update.setUpdateValues(values);

        update.setIgnore(this.isIgnore());
        update.setQuick(this.isQuick());
        update.setLowPriority(this.lowPriority);
        update.setHighPriority(this.highPriority);
        update.setDelayed(this.isDelayed());

        if (this.getNode().getActualTableName() != null) {
            update.setTableName(this.getNode().getActualTableName());
        } else if (this.getNode() instanceof KVIndexNode) {
            update.setTableName(((KVIndexNode) this.getNode()).getIndexName());
        } else {
            update.setTableName(this.getNode().getTableName());
        }
        update.setIndexName(this.getNode().getIndexUsed().getName());
        update.setBatchIndexs(this.getBatchIndexs());
        update.setMultiValues(this.isMultiValues());
        update.setMultiValues(this.getMultiValues());
        update.setExistSequenceVal(this.isExistSequenceVal());
        return update;
    }

    @Override
    public UpdateNode deepCopy() {
        UpdateNode delete = new UpdateNode(null);
        super.deepCopySelfTo(delete);
        return delete;
    }

    @Override
    public UpdateNode copy() {
        UpdateNode delete = new UpdateNode(null);
        super.copySelfTo(delete);
        return delete;
    }

    @Override
    public boolean processAutoIncrement() {
        // delete不开启
        return false;
    }

}
