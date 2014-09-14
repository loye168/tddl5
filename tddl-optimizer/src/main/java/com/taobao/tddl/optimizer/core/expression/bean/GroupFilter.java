package com.taobao.tddl.optimizer.core.expression.bean;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IGroupFilter;
import com.taobao.tddl.optimizer.exception.OptimizerException;
import com.taobao.tddl.optimizer.utils.OptimizerToString;

/**
 * @author jianghang 2014-7-3 下午3:19:39
 * @since 5.1.0
 */
public class GroupFilter extends Function<IGroupFilter> implements IGroupFilter {

    protected boolean evaled = false;

    public GroupFilter(){
        args = new ArrayList<IFilter>();
    }

    @Override
    public String getFunctionName() {
        return OPERATION.GROUP_OR.getOPERATIONString();
    }

    public IGroupFilter setOperation(OPERATION operation) {
        if (operation != OPERATION.GROUP_OR) {
            throw new NotSupportException();
        }

        return this;
    }

    @Override
    public Object getColumn() {
        List<IFilter> filter = getSubFilter();
        if (filter != null && filter.size() > 1) {
            return ((IBooleanFilter) filter.get(0)).getColumn();
        }

        return null;
    }

    @Override
    public IGroupFilter setColumn(Object column) {
        // ignore
        return this;
    }

    public OPERATION getOperation() {
        return OPERATION.GROUP_OR;
    }

    public List<IFilter> getSubFilter() {
        return args;
    }

    public IGroupFilter setSubFilter(List<IFilter> subFilters) {
        this.setArgs(subFilters);
        return this;
    }

    public IGroupFilter addSubFilter(IFilter subFilter) {
        if (!(subFilter instanceof IBooleanFilter)) {
            throw new OptimizerException("GroupFilter only support BooleanFilter");
        }

        this.args.add(subFilter);
        return this;
    }

    public IGroupFilter assignment(Parameters parameterSettings) {
        IGroupFilter filterNew = (IGroupFilter) super.assignment(parameterSettings);
        filterNew.setOperation(this.getOperation());
        return filterNew;
    }

    public String toString() {
        return OptimizerToString.printFilterString(this);
    }

    public IGroupFilter copy() {
        IGroupFilter filterNew = ASTNodeFactory.getInstance().createGroupFilter();
        super.copy(filterNew);
        filterNew.setOperation(this.getOperation());
        return filterNew;
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((args == null) ? 0 : args.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof IGroupFilter)) {
            return false;
        }
        IGroupFilter other = (IGroupFilter) obj;
        if (getOperation() != other.getOperation()) {
            return false;

        }
        if (getSubFilter() == null) {
            if (other.getSubFilter() != null) {
                return false;

            }
        } else if (!getSubFilter().equals(other.getSubFilter())) {
            return false;
        }
        return true;
    }

    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

}
