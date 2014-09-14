package com.taobao.tddl.optimizer.core.expression.bean;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.ExtraFunctionManager;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IExtraFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * 采取代理模式，将function处理转移到执行器中进行处理，比如处理分库的count/max等
 * 
 * @since 5.0.0
 */
public class Function<RT extends IFunction> implements IFunction<RT> {

    public static final Object[] emptyArgs           = new Object[0];
    // count
    protected String             functionName;
    protected List               args                = new ArrayList();
    protected String             alias;
    protected boolean            distinct            = false;
    protected String             tablename;

    // count(id)
    protected String             columnName;
    protected IExtraFunction     extraFunction;
    protected boolean            isNot;
    protected boolean            needDistinctArg     = false;
    protected Long               correlateOnFilterId = 0L;

    @Override
    public String getFunctionName() {
        return functionName;
    }

    @Override
    public IFunction setFunctionName(String functionName) {
        this.functionName = functionName;
        return this;
    }

    @Override
    public RT setDataType(DataType dataType) {
        throw new NotSupportException();
    }

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public String getTableName() {
        return tablename;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public RT setAlias(String alias) {
        this.alias = alias;
        return (RT) this;
    }

    @Override
    public RT setTableName(String tableName) {
        this.tablename = tableName;
        return (RT) this;
    }

    @Override
    public RT setColumnName(String columnName) {
        this.columnName = columnName;
        return (RT) this;
    }

    @Override
    public boolean isSameName(ISelectable select) {
        String cn1 = this.getColumnName();
        if (TStringUtil.isNotEmpty(this.getAlias())) {
            cn1 = this.getAlias();
        }

        String cn2 = select.getColumnName();
        if (TStringUtil.isNotEmpty(select.getAlias())) {
            cn2 = select.getAlias();
        }

        return TStringUtil.equals(cn1, cn2);
    }

    @Override
    public String getFullName() {
        return this.getColumnName();
    }

    @Override
    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public boolean isNot() {
        return isNot;
    }

    @Override
    public RT setDistinct(boolean distinct) {
        this.distinct = distinct;
        return (RT) this;
    }

    @Override
    public RT setIsNot(boolean isNot) {
        this.isNot = isNot;
        return (RT) this;
    }

    @Override
    public List getArgs() {
        return args;
    }

    @Override
    public RT setArgs(List args) {
        this.args = args;
        return (RT) this;
    }

    @Override
    public boolean isNeedDistinctArg() {
        return needDistinctArg;
    }

    @Override
    public RT setNeedDistinctArg(boolean b) {
        this.needDistinctArg = b;
        return (RT) this;
    }

    @Override
    public RT copy() {
        IFunction funcNew = ASTNodeFactory.getInstance().createFunction();
        funcNew.setFunctionName(getFunctionName())
            .setAlias(this.getAlias())
            .setTableName(this.getTableName())
            .setColumnName(this.getColumnName())
            .setDistinct(this.isDistinct())
            .setIsNot(this.isNot())
            .setCorrelateOnFilterId(this.getCorrelateOnFilterId());

        funcNew.setNeedDistinctArg(this.isNeedDistinctArg());
        if (getArgs() != null) {
            List<Object> argsNew = new ArrayList(getArgs().size());
            for (Object arg : getArgs()) {
                if (arg instanceof ISelectable) {
                    argsNew.add(((ISelectable) arg).copy());
                } else if (arg instanceof IBindVal) {
                    argsNew.add((((IBindVal) arg).copy()));
                } else if (arg instanceof QueryTreeNode) {
                    argsNew.add((((QueryTreeNode) arg).deepCopy()));
                } else {
                    argsNew.add(arg);
                }

            }
            funcNew.setArgs(argsNew);
        }
        return (RT) funcNew;
    }

    /**
     * 复制一下function属性
     */
    protected void copy(IFunction funcNew) {
        funcNew.setFunctionName(getFunctionName())
            .setAlias(this.getAlias())
            .setTableName(this.getTableName())
            .setColumnName(this.getColumnName())
            .setDistinct(this.isDistinct())
            .setIsNot(this.isNot())
            .setCorrelateOnFilterId(this.getCorrelateOnFilterId());

        if (getArgs() != null) {
            List<Object> argsNew = new ArrayList(getArgs().size());
            for (Object arg : getArgs()) {
                if (arg instanceof ISelectable) {
                    argsNew.add(((ISelectable) arg).copy());
                } else if (arg instanceof IBindVal) {
                    argsNew.add((((IBindVal) arg).copy()));
                } else if (arg instanceof QueryTreeNode) {
                    argsNew.add((((QueryTreeNode) arg).deepCopy()));
                } else {
                    argsNew.add(arg);
                }

            }
            funcNew.setArgs(argsNew);
        }
    }

    @Override
    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public int compareTo(Object o) {
        throw new NotSupportException();
    }

    @Override
    public RT assignment(Parameters parameterSettings) {
        if (getArgs() != null) {
            List<Object> argsNew = getArgs();
            int index = 0;
            for (Object arg : getArgs()) {
                if (arg instanceof ISelectable) {
                    argsNew.set(index, ((ISelectable) arg).assignment(parameterSettings));
                } else if (arg instanceof IBindVal) {
                    argsNew.set(index, ((IBindVal) arg).assignment(parameterSettings));
                } else {
                    argsNew.set(index, arg);
                }
                index++;
            }
            this.setArgs(argsNew);
        }

        return (RT) this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getColumnName());
        if (this.getAlias() != null) {
            builder.append(" as ").append(this.getAlias());
        }
        return builder.toString();
    }

    @Override
    public IExtraFunction getExtraFunction() {
        if (extraFunction == null) {
            extraFunction = ExtraFunctionManager.getExtraFunction(getFunctionName());
            extraFunction.setFunction(this);// 不可能为null
        }
        return extraFunction;
    }

    @Override
    public RT setExtraFunction(IExtraFunction function) {
        this.extraFunction = function;
        return (RT) this;
    }

    @Override
    public FunctionType getFunctionType() {
        return getExtraFunction().getFunctionType();
    }

    @Override
    public DataType getDataType() {
        return getExtraFunction().getReturnType();
    }

    @Override
    public Long getCorrelateOnFilterId() {
        return correlateOnFilterId;
    }

    @Override
    public RT setCorrelateOnFilterId(Long correlateOnFilterId) {
        this.correlateOnFilterId = correlateOnFilterId;
        return (RT) this;
    }

    public boolean isAutoIncrement() {
        return false;
    }

    @Override
    public RT setAutoIncrement(boolean autoIncrement) {
        return (RT) this;
    }

    /**
     * 这个方法不要被自动修改！ 在很多地方都有用到。
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((args == null) ? 0 : args.hashCode());
        result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
        result = prime * result + (distinct ? 1231 : 1237);
        result = prime * result + ((functionName == null) ? 0 : functionName.hashCode());
        result = prime * result + (isNot ? 1231 : 1237);
        result = prime * result + (needDistinctArg ? 1231 : 1237);
        result = prime * result + ((tablename == null) ? 0 : tablename.hashCode());
        return result;
    }

    /**
     * 这个方法不要被自动修改！ 在很多地方都有用到。
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Function other = (Function) obj;

        if (((alias == null && other.alias != null)) || ((alias != null && other.alias == null))) {
            return false;
        }
        // alias 都不为空的时候，进行比较，如果不匹配则返回false,其余时候都跳过alias匹配
        if (alias != null && other.alias != null) {
            if (!alias.equals(other.alias)) {
                return false;
            }
        }

        if (args == null) {
            if (other.args != null) {
                return false;
            }
        } else if (!args.equals(other.args)) {
            return false;
        }
        if (columnName == null) {
            if (other.columnName != null) {
                return false;
            }
        } else if (!columnName.equals(other.columnName)) {
            return false;
        }
        if (distinct != other.distinct) {
            return false;
        }
        if (functionName == null) {
            if (other.functionName != null) {
                return false;
            }
        } else if (!functionName.equals(other.functionName)) {
            return false;
        }
        if (isNot != other.isNot) {
            return false;
        }
        if (needDistinctArg != other.needDistinctArg) {
            return false;
        }
        if (tablename == null) {
            if (other.tablename != null) {
                return false;
            }
        } else if (!tablename.equals(other.tablename)) {
            return false;
        }
        return true;
    }
}
