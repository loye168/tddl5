package com.taobao.tddl.repo.mysql.function.any;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.executor.function.scalar.filter.Filter;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.repo.mysql.function.FunctionStringConstructor;
import com.taobao.tddl.repo.mysql.sqlconvertor.MysqlPlanVisitorImpl;

public class NotEqualAny implements FunctionStringConstructor {

    @Override
    public String constructColumnNameForFunction(IDataNodeExecutor query, boolean bindVal,
                                                 AtomicInteger bindValSequence,
                                                 Map<Integer, ParameterContext> paramMap, IFunction func,
                                                 MysqlPlanVisitorImpl parentVisitor) {
        StringBuilder sb = new StringBuilder();

        Object right = func.getArgs().get(1);

        if (right instanceof List) {
            boolean isAllEqual = true;
            List rightList = (List) right;

            if (rightList.size() == 0) {
                sb.append(" (true) ");
                return sb.toString();
            }
            DataType type = ((Filter) func.getExtraFunction()).getArgType();
            for (int i = 0; i < rightList.size() - 1; i++) {
                if (type.compare(rightList.get(i), rightList.get(i + 1)) != 0) {
                    isAllEqual = false;
                    break;
                }
            }

            if (isAllEqual) {
                sb.append(parentVisitor.getNewVisitor(func.getArgs().get(0)).getString());
                sb.append(" != ");
                sb.append(parentVisitor.getNewVisitor(rightList.get(0)).getString());
            } else {
                // 如果list中的值不全相等，那么存在一个右值跟左值不相等了
                sb.append(" (true) ");
            }

        } else {

            sb.append(parentVisitor.getNewVisitor(func.getArgs().get(0)).getString());

            // 右边没进行计算，还是个子查询，理论上不会发生
            sb.append(" !=any (");
            sb.append(parentVisitor.getNewVisitor(func.getArgs().get(1)).getString());
            sb.append(")");
        }

        return sb.toString();

    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { OPERATION.NOT_EQ_ANY.getOPERATIONString() };
    }
}
