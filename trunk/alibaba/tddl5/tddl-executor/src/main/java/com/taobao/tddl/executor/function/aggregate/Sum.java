package com.taobao.tddl.executor.function.aggregate;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.AggregateFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class Sum extends AggregateFunction {

    public Sum(){
    }

    @Override
    public void serverMap(Object[] args, ExecutionContext ec) {
        doSum(args);
    }

    @Override
    public void serverReduce(Object[] args, ExecutionContext ec) {
        doSum(args);
    }

    private void doSum(Object[] args) {
        Object o = args[0];

        if (o == null) return;
        DataType type = this.getMapReturnType();

        if (result == null) {
            o = type.convertFrom(o);
            result = o;
        } else {
            result = type.getCalculator().add(result, o);
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
        return DataType.BigDecimalType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SUM" };
    }
}
