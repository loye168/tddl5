package com.taobao.tddl.optimizer.core.plan.bean;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;

import java.util.List;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.dml.IInsert;
import com.taobao.tddl.optimizer.utils.OptimizerToString;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public class Insert extends Put<IInsert> implements IInsert {

    private List<ISelectable> duplicateUpdateColumns;
    private List<Object>      duplicateUpdateValues;

    public Insert(){
        putType = PUT_TYPE.INSERT;
    }

    @Override
    public void setDuplicateUpdateColumns(List<ISelectable> cs) {
        this.duplicateUpdateColumns = cs;
    }

    @Override
    public List<Object> getDuplicateUpdateValues() {
        return duplicateUpdateValues;
    }

    @Override
    public List<ISelectable> getDuplicateUpdateColumns() {
        return duplicateUpdateColumns;
    }

    @Override
    public void setDuplicateUpdateValues(List<Object> valueList) {
        this.duplicateUpdateValues = valueList;
    }

    @Override
    public IInsert copy() {
        IInsert insert = ASTNodeFactory.getInstance().createInsert();
        copySelfTo(insert);
        insert.setDuplicateUpdateColumns(OptimizerUtils.copySelectables(duplicateUpdateColumns));
        insert.setDuplicateUpdateValues(OptimizerUtils.copyValues(duplicateUpdateValues));
        return insert;
    }

    @Override
    public String toStringWithInden(int inden, ExplainMode mode) {
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder(super.toStringWithInden(inden, mode));
        appendField(sb, "duplicateColumns", this.getDuplicateUpdateColumns(), tabContent);
        appendField(sb, "duplicateValues", this.getDuplicateUpdateValues(), tabContent);
        return sb.toString();
    }
}
