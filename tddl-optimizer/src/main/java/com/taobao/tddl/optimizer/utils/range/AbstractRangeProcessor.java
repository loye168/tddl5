package com.taobao.tddl.optimizer.utils.range;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * @author jianghang 2013-11-13 下午4:11:25
 * @since 5.0.0
 */
public abstract class AbstractRangeProcessor {

    /**
     * 处理下filter
     */
    public abstract boolean process(IFilter f);

    /**
     * 构造Range对象
     * 
     * @param f
     * @return
     */
    protected Range getRange(IFilter filter) {
        if (!(filter instanceof IBooleanFilter)) {
            return null;
        }

        IBooleanFilter f = (IBooleanFilter) filter;
        if (FilterUtils.isConstFilter(f)) {
            return null;
        }

        if (filter.getOperation() == OPERATION.IN) {
            // 不处理null
            return null;
        }

        DataType type = getColumn(f).getDataType();
        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(f.getValue());
        }

        if (!isNumberType(type)) {
            // 只处理数字类型，避免时间戳类型的合并
            return null;
        }

        switch (f.getOperation()) {
            case EQ:
                return new Range(null, type, getValue(f), getValue(f));
            case GT:
                return new Range(null, type, getValue(f), false, null, true);
            case GT_EQ:
                return new Range(null, type, getValue(f), true, null, true);
            case LT:
                return new Range(null, type, null, true, getValue(f), false);
            case LT_EQ:
                return new Range(null, type, null, true, getValue(f), true);
            default:
                return null;
        }

    }

    protected boolean isNumberType(DataType type) {
        if (type == null) {
            return false;
        }
        if (type == DataType.IntegerType || type == DataType.LongType || type == DataType.ShortType
            || type == DataType.BigIntegerType || type == DataType.BigDecimalType) {

            return true;
        } else {
            return false;
        }
    }

    protected Comparable getValue(IBooleanFilter f) {
        return (Comparable) f.getValue();
    }

    protected ISelectable getColumn(IBooleanFilter f) {
        return (ISelectable) f.getColumn();
    }

    /**
     * 根据range结果，构造filter
     * 
     * @param range
     * @param column
     * @return
     */
    protected List<IFilter> buildFilter(Range range, Object column) {
        List<IFilter> filters = new ArrayList(2);
        if (range == null) {
            return filters;
        }

        if (range.isSingleValue()) {
            IBooleanFilter en = ASTNodeFactory.getInstance().createBooleanFilter().setOperation(OPERATION.EQ);
            en.setColumn(column);
            en.setValue(range.getMaxValue());
            filters.add(en);
            return filters;
        }

        if (range.getMinValue() != null) {
            IBooleanFilter gn;
            if (range.isMinIncluded()) {
                gn = ASTNodeFactory.getInstance().createBooleanFilter().setOperation(OPERATION.GT_EQ);
            } else {
                gn = ASTNodeFactory.getInstance().createBooleanFilter().setOperation(OPERATION.GT);
            }

            gn.setColumn(column);
            gn.setValue(range.getMinValue());
            filters.add(gn);
        }

        if (range.getMaxValue() != null) {
            IBooleanFilter ln;
            if (range.isMaxIncluded()) {
                ln = ASTNodeFactory.getInstance().createBooleanFilter().setOperation(OPERATION.LT_EQ);
            } else {
                ln = ASTNodeFactory.getInstance().createBooleanFilter().setOperation(OPERATION.LT);
            }

            ln.setColumn(column);
            ln.setValue(range.getMaxValue());
            filters.add(ln);
        }
        return filters;
    }
}
