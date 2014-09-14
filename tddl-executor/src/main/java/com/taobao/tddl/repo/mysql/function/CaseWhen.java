package com.taobao.tddl.repo.mysql.function;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.repo.mysql.sqlconvertor.MysqlPlanVisitorImpl;

public class CaseWhen implements FunctionStringConstructor {

    @Override
    public String constructColumnNameForFunction(IDataNodeExecutor query, boolean bindVal,
                                                 AtomicInteger bindValSequence,
                                                 Map<Integer, ParameterContext> paramMap, IFunction func,
                                                 MysqlPlanVisitorImpl parentVisitor) {
        StringBuilder sb = new StringBuilder();
        // case
        sb.append("case").append(" ");
        boolean hasComparee = (Boolean) func.getArgs().get(0);

        // case id
        if (hasComparee) {
            sb.append(parentVisitor.getNewVisitor(func.getArgs().get(1)).getString());
            sb.append(" ");
        }

        for (int i = 2; i < func.getArgs().size() - 2; i = i + 2) {
            // case id when
            sb.append("when").append(" ");

            // case id when 10
            sb.append(parentVisitor.getNewVisitor(func.getArgs().get(i)).getString());
            sb.append(" ");

            // case id when 10 then
            sb.append("then").append(" ");

            // case id when 10 then 100
            sb.append(parentVisitor.getNewVisitor(func.getArgs().get(i + 1)).getString());
            sb.append(" ");
        }

        boolean hasElseResult = (Boolean) func.getArgs().get(func.getArgs().size() - 2);

        // case id when 10 then 100 else 10
        if (hasElseResult) {
            sb.append("else").append(" ");
            sb.append(parentVisitor.getNewVisitor(func.getArgs().get(func.getArgs().size() - 1)).getString());
            sb.append(" ");
        }

        // case id when 10 then 100 else 10 end
        sb.append("end").append(" ");

        return sb.toString();

    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { IFunction.BuiltInFunction.CASE_WHEN };
    }

}
