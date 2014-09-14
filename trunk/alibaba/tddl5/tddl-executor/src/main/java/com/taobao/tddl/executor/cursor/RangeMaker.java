package com.taobao.tddl.executor.cursor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.impl.ColMetaAndIndex;
import com.taobao.tddl.executor.cursor.impl.CursorMetaImp;
import com.taobao.tddl.executor.rowset.ArrayRowSet;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IGroupFilter;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;

public class RangeMaker {

    private ExecutionContext ec;

    public RangeMaker(ExecutionContext ec){
        this.ec = ec;
    }

    public static class Range {

        public IRowSet from;
        public IRowSet to;
    }

    public RangeMaker.Range makeRange(IFilter lf, List<IOrderBy> orderBys) {
        RangeMaker.Range range = new RangeMaker.Range();

        ICursorMeta cursorMetaNew = getICursorMetaByOrderBy(orderBys);
        range.to = new ArrayRowSet(orderBys.size(), cursorMetaNew);
        //
        range.from = new ArrayRowSet(orderBys.size(), cursorMetaNew);

        convertFilterToLowerAndTopLimit(range, lf, cursorMetaNew);

        boolean min = false;
        fillRowSet(range.from, min);
        boolean max = true;
        fillRowSet(range.to, max);
        return range;
    }

    public ICursorMeta getICursorMetaByOrderBy(List<IOrderBy> orderBys) {
        List<ColumnMeta> columnsNew = new ArrayList<ColumnMeta>(orderBys.size());
        for (IOrderBy orderBy : orderBys) {
            ColumnMeta cmNew = ExecUtils.getColumnMeta(orderBy.getColumn());
            columnsNew.add(cmNew);
        }

        ICursorMeta cursorMetaNew = CursorMetaImp.buildNew(columnsNew, orderBys.size());
        return cursorMetaNew;
    }

    protected void convertFilterToLowerAndTopLimit(RangeMaker.Range range, IFilter lf, ICursorMeta cursorMetaNew) {
        if (lf instanceof IBooleanFilter) {
            processBoolfilter(range, lf, cursorMetaNew);
        } else if (lf instanceof ILogicalFilter) {
            ILogicalFilter lo = (ILogicalFilter) lf;
            if (lo.getOperation() == OPERATION.OR) {
                throw new IllegalStateException("or ? should not be here");
            }
            List<IFilter> list = lo.getSubFilter();
            for (IFilter filter : list) {
                convertFilterToLowerAndTopLimit(range, filter, cursorMetaNew);
            }
        } else if (lf instanceof IGroupFilter) {
            throw new IllegalStateException("or ? should not be here");
        }
    }

    /**
     * 将大于等于 变成大于 小于 变成小于等于 将
     * 
     * @param lf
     * @param cursorMetaNew
     */
    protected void processBoolfilter(RangeMaker.Range range, IFilter lf, ICursorMeta cursorMetaNew) {
        IBooleanFilter bf = (IBooleanFilter) lf;
        switch (bf.getOperation()) {
            case GT_EQ:
                setIntoRowSet(cursorMetaNew, bf, range.from, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return c;
                    }
                });
                break;
            case GT:
                setIntoRowSet(cursorMetaNew, bf, range.from, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return incr(c, type);
                    }
                });
                break;
            case LT:
                setIntoRowSet(cursorMetaNew, bf, range.to, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return decr(c, type);
                    }
                });
                break;
            case LT_EQ:
                setIntoRowSet(cursorMetaNew, bf, range.to, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return c;
                    }
                });
                break;
            case EQ:
                setIntoRowSet(cursorMetaNew, bf, range.from, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return c;
                    }
                });
                setIntoRowSet(cursorMetaNew, bf, range.to, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return c;
                    }
                });
                break;
            default:
                throw new IllegalArgumentException("not supported yet");
        }
    }

    protected void setIntoRowSet(ICursorMeta cursorMetaNew, IBooleanFilter bf, IRowSet rowSet,
                                 ColumnEQProcessor columnProcessor) {
        IColumn col = ExecUtils.getIColumn(bf.getColumn());
        Object val = bf.getValue();

        val = processFunction(val);
        val = columnProcessor.process(val, col.getDataType());
        Integer inte = cursorMetaNew.getIndex(col.getTableName(), col.getColumnName(), col.getAlias());

        rowSet.setObject(inte, val);
    }

    public Object decr(Object c, DataType type) {
        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(c); // 可能为null
        }
        return type.decr(c);
    }

    /**
     * 用来做一些值的处理工作
     * 
     * @author whisper
     */
    public static interface ColumnEQProcessor {

        public Object process(Object c, DataType type);
    }

    public Object incr(Object c, DataType type) {
        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(c); // 可能为null
        }

        return type.incr(c);
    }

    @SuppressWarnings("rawtypes")
    protected Comparable processFunction(Object v) {
        if (v instanceof IFunction) {
            // 要计算还是一并放在最前边计算好
            throw new IllegalAccessError("impossible");
        }
        return (Comparable) v;
    }

    protected void fillRowSet(IRowSet iRowSet, boolean max) {
        ICursorMeta icm = iRowSet.getParentCursorMeta();
        Iterator<ColMetaAndIndex> iterator = icm.indexIterator();
        while (iterator.hasNext()) {
            ColMetaAndIndex cai = iterator.next();
            String cname = cai.getName();
            List<ColumnMeta> columns = iRowSet.getParentCursorMeta().getColumns();
            // TODO shenxun : 如果条件是null。。。没处理。。应该在Comparator里面处理
            if (iRowSet.getObject(cai.getIndex()) == null) {
                boolean find = false;
                for (ColumnMeta cm : columns) {
                    if (cname.equalsIgnoreCase(cm.getName())) {
                        find = true;
                        iRowSet.setObject(cai.getIndex(), getExtremum(max, cm.getDataType()));
                        break;
                    }
                }
                if (!find) {
                    throw new IllegalStateException(" can't find column name : " + cname + " on . " + columns);
                }
            }
        }
    }

    public Object getExtremum(boolean max, DataType type) {
        if (max) {
            return type.getMaxValue();
        } else {
            // 任何类型的最小值都是null;
            return type.getMinValue();
        }
    }

    public ExecutionContext getExecutionContext() {
        return ec;
    }

}
