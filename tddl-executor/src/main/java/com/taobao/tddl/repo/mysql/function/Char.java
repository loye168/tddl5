package com.taobao.tddl.repo.mysql.function;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.repo.mysql.sqlconvertor.MysqlPlanVisitorImpl;

public class Char implements FunctionStringConstructor {

    @Override
    public String constructColumnNameForFunction(IDataNodeExecutor query, boolean bindVal,
                                                 AtomicInteger bindValSequence,
                                                 Map<Integer, ParameterContext> paramMap, IFunction func,
                                                 MysqlPlanVisitorImpl parentVisitor) {
        StringBuilder sb = new StringBuilder();

        sb.append(IFunction.BuiltInFunction.CHAR).append("(");
        int size = func.getArgs().size();
        for (int i = 0; i < size - 1; i++) {
            // typeinfo1
            sb.append(parentVisitor.getNewVisitor(func.getArgs().get(i)).getString());
            if (i < size - 2) {
                sb.append(",");
            }
        }

        size = func.getArgs().size();
        if (!ExecUtils.isNull(func.getArgs().get(size - 1))) {
            sb.append(" USING ").append(func.getArgs().get(size - 1));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { IFunction.BuiltInFunction.CHAR };
    }
}
