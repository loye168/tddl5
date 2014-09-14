package com.taobao.tddl.executor.function.aggregate;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.AggregateFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;

/**
 * @since 5.0.0
 */
public class Max extends AggregateFunction {

    public Max(){
    }

    @Override
    public void serverMap(Object[] args, ExecutionContext ec) {
        doMax(args);
    }

    @Override
    public void serverReduce(Object[] args, ExecutionContext ec) {
        doMax(args);
    }

    private void doMax(Object[] args) {
        Object o = args[0];
        DataType type = this.getReturnType();

        if (type == null) {
            if (o != null) {
                type = DataTypeUtil.getTypeOfObject(o);
            }
        }
        if (o != null) {
            if (result == null) {
                result = o;
            }
            if (type.compare(o, result) > 0) {
                result = o;
            }

        }
    }

    public int getArgSize() {
        return 1;
    }

    @Override
    public DataType getReturnType() {
        return this.getMapReturnType();
    }

    @Override
    public DataType getMapReturnType() {
        return getFirstArgType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "MAX" };
    }
}
