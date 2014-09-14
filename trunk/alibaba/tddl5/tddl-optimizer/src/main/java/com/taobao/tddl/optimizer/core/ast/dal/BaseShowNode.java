package com.taobao.tddl.optimizer.core.ast.dal;

import java.util.List;

import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IGroupFilter;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

public abstract class BaseShowNode extends ASTNode {

    public enum ShowType {

        SEQUENCES, PARTITIONS, TABLES, TOPOLOGY, BRAODCASTS, RULE, CREATE_TABLE, DESC, INDEX, INDEXES, KEYS, COLUMNS,
        TRACE, DATASOURCES;

    }

    protected ShowType type;
    protected IFilter  whereFilter;
    protected String   pattern;
    protected boolean  full;

    public ShowType getType() {
        return type;
    }

    public void setType(ShowType type) {
        this.type = type;
    }

    public IFilter getWhereFilter() {
        return whereFilter;
    }

    public void setWhereFilter(IFilter whereFilter) {
        this.whereFilter = whereFilter;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

    protected void buildFilter(IFilter filter) {
        if (filter == null) {
            return;
        }

        if (filter instanceof ILogicalFilter) {
            for (IFilter sub : ((ILogicalFilter) filter).getSubFilter()) {
                this.buildFilter(sub);
            }
        } else if (filter instanceof IGroupFilter) {
            for (IFilter sub : ((IGroupFilter) filter).getSubFilter()) {
                this.buildFilter(sub);
            }
        } else {
            buildBooleanFilter((IBooleanFilter) filter);
        }
    }

    protected void buildBooleanFilter(IBooleanFilter filter) {
        if (filter == null) {
            return;
        }

        Object column = filter.getColumn();
        Object value = filter.getValue();

        if (column instanceof ISelectable) {
            filter.setColumn(this.buildSelectable((ISelectable) column));
        }

        if (value instanceof ISelectable) {
            filter.setValue(this.buildSelectable((ISelectable) value));
        }

        if (value != null && value instanceof IFunction && ((IFunction) value).getArgs().size() > 0) {
            Object arg = ((IFunction) value).getArgs().get(0);
            if (arg instanceof QueryTreeNode) {
            }
        }

    }

    public ISelectable buildSelectable(ISelectable c) {
        if (c == null) {
            return null;
        }

        ISelectable column = null;
        ISelectable columnFromMeta = null;

        if (column == null) {// 查找table meta
            columnFromMeta = this.getSelectableFromChild(c);

            if (columnFromMeta != null) {
                column = columnFromMeta;
                // 直接从子类的table定义中获取表字段，然后根据当前column状态，设置alias和distinct
                column.setAlias(c.getAlias());
                column.setDistinct(c.isDistinct());
            }
        }

        if (column instanceof IFunction) {

            buildFunction((IFunction) column);

        }

        return column;
    }

    private ISelectable getSelectableFromChild(ISelectable c) {
        c.setDataType(DataType.StringType);
        return c;
    }

    protected void buildFunction(IFunction f) {
        if (f.getArgs().size() == 0) {
            return;
        }

        List<Object> args = f.getArgs();
        for (int i = 0; i < args.size(); i++) {
            args.set(i, this.buildSelectable((ISelectable) args.get(i)));

        }
    }

    @Override
    public void build() {
        if (this.whereFilter != null) {
            buildFilter(whereFilter);
        }
    }

}
