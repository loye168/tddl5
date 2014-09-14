package com.taobao.tddl.executor.function.scalar.filter;

import java.util.Collection;
import java.util.List;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.scalar.filter.Row.RowValue;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class In extends Filter {

    @Override
    protected Boolean computeInner(Object[] args, ExecutionContext ec) {
        DataType type = this.getArgType();

        Object left = args[0];
        Object right = args[1];

        if (right instanceof List) {
            if (!GeneralUtil.isEmpty((Collection) right)) {
                if (((List) right).get(0) instanceof List) {
                    right = ((List) right).get(0);
                }
            }
            for (Object eachRight : (List) right) {
                // 是否出现(id,name) in ((1,'a'),(2,'b'))
                if (left instanceof RowValue) {
                    if (!(eachRight instanceof RowValue)) {
                        throw new NotSupportException("impossible");
                    }

                    List<Object> leftArgs = ((RowValue) left).getValues();
                    List<Object> rightArgs = ((RowValue) eachRight).getValues();
                    if (leftArgs.size() != rightArgs.size()) {
                        throw new NotSupportException("impossible");
                    }

                    boolean notMatch = false;
                    for (int i = 0; i < leftArgs.size(); i++) {
                        Object leftArg = leftArgs.get(i);
                        Object rightArg = rightArgs.get(i);

                        if (getArgType(leftArg).compare(leftArg, rightArg) != 0) {
                            // 不匹配
                            notMatch = true;
                            break;
                        }
                    }

                    if (!notMatch) {
                        return true;
                    }
                } else {
                    if (type.compare(left, eachRight) == 0) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "IN" };
    }
}
