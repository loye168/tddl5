package com.taobao.tddl.repo.mysql.function.all;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.repo.mysql.function.FunctionStringConstructor;
import com.taobao.tddl.repo.mysql.sqlconvertor.MysqlPlanVisitorImpl;

public class NotEqualAll implements FunctionStringConstructor {

    @Override
    public String constructColumnNameForFunction(IDataNodeExecutor query, boolean bindVal,
                                                 AtomicInteger bindValSequence,
                                                 Map<Integer, ParameterContext> paramMap, IFunction func,
                                                 MysqlPlanVisitorImpl parentVisitor) {
        StringBuilder sb = new StringBuilder();

        sb.append(parentVisitor.getNewVisitor(func.getArgs().get(0)).getString());

        Object right = func.getArgs().get(1);

        // 右边被提前计算过了，直接用not in
        if (right instanceof List) {
            sb.append(" not in ");
            sb.append(parentVisitor.getNewVisitor(func.getArgs().get(1)).getString());
        } else {
            // 右边没进行计算，还是个子查询，理论上不会发生
            sb.append(" != all (");
            sb.append(parentVisitor.getNewVisitor(func.getArgs().get(1)).getString());
            sb.append(")");
        }

        return sb.toString();

    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { OPERATION.NOT_EQ_ALL.getOPERATIONString() };
    }
}
