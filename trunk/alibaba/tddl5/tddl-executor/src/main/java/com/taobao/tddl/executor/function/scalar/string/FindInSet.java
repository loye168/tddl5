package com.taobao.tddl.executor.function.scalar.string;

import java.util.Arrays;
import java.util.List;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

public class FindInSet extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.IntegerType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "FIND_IN_SET" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (ExecUtils.isNull(args[0]) || ExecUtils.isNull(args[1])) {
            return null;
        }

        String str1 = DataType.StringType.convertFrom(args[0]);
        List strList = Arrays.asList((TStringUtil.split(DataType.StringType.convertFrom(args[1]), ',')));

        return strList.indexOf(str1) + 1;

    }

}
