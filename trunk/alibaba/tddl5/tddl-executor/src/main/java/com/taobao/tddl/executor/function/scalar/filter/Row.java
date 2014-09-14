package com.taobao.tddl.executor.function.scalar.filter;

import java.util.Arrays;
import java.util.List;

import com.taobao.tddl.executor.common.ExecutionContext;

/**
 * @since 5.0.0
 */
public class Row extends Filter {

    @Override
    protected Object computeInner(Object[] args, ExecutionContext ec) {
        return new RowValue(Arrays.asList(args));
    }

    public static class RowValue {

        private List<Object> values;

        public RowValue(List<Object> values){
            this.values = values;
        }

        public List<Object> getValues() {
            return values;
        }

        public void setValues(List<Object> values) {
            this.values = values;
        }

    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "ROW" };
    }
}
