package com.taobao.tddl.repo.hbase.cursor;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.RangeMaker;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;

public class RangeMakerForHBase extends RangeMaker {

    public RangeMakerForHBase(ExecutionContext ec){
        super(ec);
    }

    @Override
    public Object getExtremum(boolean max, DataType type) {
        if (max) {
            return new MaxValue();
        } else {
            // 任何类型的最小值都是null;
            return new MinValue();
        }
        // return null;
    }

    /**
     * 将大于等于 变成大于 小于 变成小于等于 将
     * 
     * @param lf
     * @param cursorMetaNew
     */
    @Override
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
                        return c;
                    }
                });
                break;
            case LT_EQ:
                setIntoRowSet(cursorMetaNew, bf, range.to, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return incr(c, type);
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
}
