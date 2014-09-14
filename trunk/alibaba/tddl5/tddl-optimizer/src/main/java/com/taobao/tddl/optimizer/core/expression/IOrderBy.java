package com.taobao.tddl.optimizer.core.expression;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.CanVisit;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public interface IOrderBy extends CanVisit {

    public IOrderBy setColumn(ISelectable columnName);

    public ISelectable getColumn();

    public Boolean getDirection();

    public IOrderBy assignment(Parameters parameterSettings);

    public IOrderBy setDirection(Boolean direction);

    public String getAlias();

    public IOrderBy setTableName(String alias);

    public IOrderBy setColumnName(String alias);

    public String getTableName();

    public String getColumnName();

    public DataType getDataType();

    public String toStringWithInden(int inden);

    public IOrderBy copy();

}
