package com.taobao.tddl.optimizer.core.plan.dml;

import java.util.List;

import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IPut;

/**
 * @since 5.0.0
 */
public interface IInsert extends IPut<IInsert> {

    void setDuplicateUpdateColumns(List<ISelectable> duplicateUpdateColumns);

    void setDuplicateUpdateValues(List<Object> duplicateUpdateValues);

    public List<Object> getDuplicateUpdateValues();

    public List<ISelectable> getDuplicateUpdateColumns();

}
