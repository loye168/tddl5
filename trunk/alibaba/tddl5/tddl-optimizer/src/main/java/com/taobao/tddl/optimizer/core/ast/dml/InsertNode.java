package com.taobao.tddl.optimizer.core.ast.dml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.dml.IInsert;
import com.taobao.tddl.optimizer.rule.OptimizerRule;
import com.taobao.tddl.optimizer.utils.OptimizerToString;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public class InsertNode extends DMLNode<InsertNode> {

    private boolean           createPk = true;       // 是否为自增长字段，暂时不支持
    private List<Object>      duplicateUpdateValues;
    private List<ISelectable> duplicateUpdateColumns;

    public InsertNode(TableNode table){
        super(table);
    }

    @Override
    public IPut toDataNodeExecutor(int shareIndex) {
        IInsert insert = ASTNodeFactory.getInstance().createInsert();
        if (this.getDuplicateUpdateColumns() != null) {
            List<String> partitionColumns = OptimizerContext.getContext()
                .getRule()
                .getSharedColumns(this.getNode().getTableMeta().getTableName());

            for (ISelectable updateColumn : this.getDuplicateUpdateColumns()) {
                if (partitionColumns.contains(updateColumn.getColumnName())) {
                    throw new IllegalArgumentException("column :" + updateColumn.getColumnName()
                                                       + " is partition key , can't be modify");
                }

                if (this.getNode().getTableMeta().getPrimaryKeyMap().containsKey(updateColumn.getColumnName())) {
                    throw new IllegalArgumentException("column :" + updateColumn.getColumnName()
                                                       + " is primary key , can't be modify");
                }
            }
        }

        if (this.getNode().getActualTableName() != null) {
            insert.setTableName(this.getNode().getActualTableName());
        } else if (this.getNode() instanceof KVIndexNode) {
            insert.setTableName(((KVIndexNode) this.getNode()).getIndexName());
        } else {
            insert.setTableName(this.getNode().getTableName());
        }
        insert.setIndexName((this.getNode()).getIndexUsed().getName());
        insert.setConsistent(true);
        insert.setUpdateColumns(this.getColumns());
        insert.setUpdateValues(this.getValues());
        if (this.getSelectNode() != null) {
            insert.setQueryTree((IQueryTree) this.getSelectNode().toDataNodeExecutor());
        }

        insert.setIgnore(this.isIgnore());
        insert.setQuick(this.isQuick());
        insert.setLowPriority(this.lowPriority);
        insert.setHighPriority(this.highPriority);
        insert.setDelayed(this.isDelayed());

        insert.setDuplicateUpdateColumns(this.getDuplicateUpdateColumns());
        insert.setDuplicateUpdateValues(this.getDuplicateUpdateValues());
        insert.setMultiValues(this.isMultiValues());
        insert.setMultiValues(this.getMultiValues());

        insert.executeOn(this.getNode().getDataNode());
        insert.setBatchIndexs(this.getBatchIndexs());
        insert.setExistSequenceVal(this.isExistSequenceVal());
        return insert;
    }

    @Override
    public InsertNode deepCopy() {
        InsertNode insert = new InsertNode(null);
        super.deepCopySelfTo(insert);
        insert.setCreatePk(this.isCreatePk());

        insert.setDuplicateUpdateColumns(OptimizerUtils.copySelectables(this.duplicateUpdateColumns));
        insert.setDuplicateUpdateValues(OptimizerUtils.copyValues(this.duplicateUpdateValues));
        return insert;
    }

    @Override
    public InsertNode copy() {
        InsertNode insert = new InsertNode(null);
        super.copySelfTo(insert);
        insert.setCreatePk(this.isCreatePk());
        insert.setDuplicateUpdateColumns(this.getDuplicateUpdateColumns());
        insert.setDuplicateUpdateValues(this.getDuplicateUpdateValues());
        insert.setMultiValues(this.isMultiValues());
        insert.setMultiValues(multiValues);
        return insert;
    }

    public boolean isCreatePk() {
        return createPk;
    }

    public InsertNode setCreatePk(boolean createPk) {
        this.createPk = createPk;
        return this;
    }

    public void duplicateUpdate(String[] updateColumns, Object[] updateValues) {
        List<ISelectable> cs = new LinkedList<ISelectable>();
        for (String name : updateColumns) {
            ISelectable s = OptimizerUtils.createColumnFromString(name);
            cs.add(s);
        }

        List<Object> valueList = new ArrayList<Object>(Arrays.asList(updateValues));

        this.setDuplicateUpdateColumns(cs);
        this.setDuplicateUpdateValues(valueList);

    }

    public void setDuplicateUpdateColumns(List<ISelectable> cs) {
        this.duplicateUpdateColumns = cs;

    }

    public List<Object> getDuplicateUpdateValues() {
        return duplicateUpdateValues;
    }

    public List<ISelectable> getDuplicateUpdateColumns() {
        return duplicateUpdateColumns;
    }

    public void setDuplicateUpdateValues(List<Object> valueList) {
        this.duplicateUpdateValues = valueList;

    }

    @Override
    public void assignment(Parameters parameterSettings) {
        super.assignment(parameterSettings);
        if (duplicateUpdateValues != null) {
            this.setDuplicateUpdateValues(assignmentValues(duplicateUpdateValues, parameterSettings));
        }
    }

    @Override
    public void build() {
        super.build();
        if (this.getDuplicateUpdateColumns() != null) {
            for (ISelectable s : this.getDuplicateUpdateColumns()) {
                ISelectable res = null;
                for (Object obj : table.getColumnsReferedForParent()) {
                    ISelectable querySelected = (ISelectable) obj;
                    if (s.isSameName(querySelected)) { // 尝试查找对应的字段信息
                        res = querySelected;
                        break;
                    }
                }

                if (res == null) {
                    throw new IllegalArgumentException("column: " + s.getColumnName() + " is not existed in "
                                                       + table.getName());
                }

                s.setTableName(res.getTableName());
                s.setDataType(res.getDataType());
                s.setAutoIncrement(res.isAutoIncrement());
            }

            convertTypeToSatifyColumnMeta(this.getDuplicateUpdateColumns(), this.getDuplicateUpdateValues());
        }
    }

    @Override
    public String toString(int inden, int shareIndex) {
        String tabContent = OptimizerToString.getTab(inden + 1);
        String str = super.toString(inden, shareIndex);
        StringBuilder sb = new StringBuilder(str);
        OptimizerToString.appendField(sb, "duplicateUpdateColumns", this.getDuplicateUpdateColumns(), tabContent);
        OptimizerToString.appendField(sb, "duplicateUpdateValues", this.getDuplicateUpdateValues(), tabContent);
        return sb.toString();
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
