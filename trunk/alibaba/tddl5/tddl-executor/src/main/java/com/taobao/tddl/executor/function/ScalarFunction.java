package com.taobao.tddl.executor.function;

import java.util.List;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.exception.ExecutorException;
import com.taobao.tddl.executor.exception.FunctionException;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.core.expression.IExtraFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;

/**
 * map是分发过程，reduce是合并过程。<br/>
 * 分发和合并都是在计算节点上进行的（计算节点在客户端内，包含数据节点、合并节点和客户端节点） 其余的与map reduce模式一致。
 * 
 * @author Whisper
 * @author jianghang 2013-11-8 下午3:42:52
 * @since 5.0.0
 */
public abstract class ScalarFunction extends ExtraFunction implements IExtraFunction {

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.Scalar;
    }

    @Override
    public String getDbFunction() {
        return function.getColumnName();
    }

    protected abstract Object compute(Object[] args, ExecutionContext ec);

    /**
     * @param kvPair
     * @param ec
     * @throws TddlRuntimeException
     */
    public Object scalarCalucate(IRowSet kvPair, ExecutionContext ec) {
        // 当前function需要的args 有些可能是函数，也有些是其他的一些数据
        List<Object> argsArr = getMapArgs(function);
        // 函数的input参数
        Object[] inputArg = new Object[argsArr.size()];
        int index = 0;
        for (Object funcArg : argsArr) {
            inputArg[index] = getArgValue(funcArg, kvPair, ec);
            index++;
        }

        if (this instanceof ScalarFunction) {
            try {
                return this.compute(inputArg, ec);
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new FunctionException("Incorrect parameter count in the call to native function '"
                                            + this.function.getFunctionName() + "'");
            }
        } else {
            throw new ExecutorException("impossible");
        }
    }

    @Override
    public void clear() {

    }

}
