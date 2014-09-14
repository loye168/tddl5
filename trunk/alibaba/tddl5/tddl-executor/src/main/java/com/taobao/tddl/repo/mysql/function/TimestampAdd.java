package com.taobao.tddl.repo.mysql.function;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.repo.mysql.sqlconvertor.MysqlPlanVisitorImpl;

public class TimestampAdd implements FunctionStringConstructor {

    @Override
    public String constructColumnNameForFunction(IDataNodeExecutor query, boolean bindVal,
                                                 AtomicInteger bindValSequence,
                                                 Map<Integer, ParameterContext> paramMap, IFunction func,
                                                 MysqlPlanVisitorImpl parentVisitor) {
        StringBuilder sb = new StringBuilder();

        sb.append(IFunction.BuiltInFunction.TIMESTAMPADD).append("(");
        sb.append(func.getArgs().get(0));
        sb.append(",");
        sb.append(parentVisitor.getNewVisitor(func.getArgs().get(1)).getString());
        sb.append(",");
        sb.append(parentVisitor.getNewVisitor(func.getArgs().get(2)).getString());
        sb.append(")");

        return sb.toString();

    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { IFunction.BuiltInFunction.TIMESTAMPADD };
    }

}
