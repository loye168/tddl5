package com.taobao.tddl.executor.function.subquery;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.exception.ExecutorException;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;

public class SubqueryScalar extends AbstractSubQueryFunction {

    @Override
    public String[] getFunctionNames() {
        return new String[] { IFunction.BuiltInFunction.SUBQUERY_SCALAR };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        IQueryTree plan = this.getQueryPlan(args);
        ISchematicCursor rc = null;
        try {
            Object result = null;
            rc = ExecutorContext.getContext().getTopologyExecutor().execByExecPlanNode(plan, ec);
            IRowSet row = rc.next();
            if (row != null) {
                ColumnMeta cm = row.getParentCursorMeta().getColumns().get(0);
                result = ExecUtils.getValueByColumnMeta(row, cm);
            } else {
                result = null;
            }

            if (rc.next() != null) {
                throw new ExecutorException("sub query return more than one row");
            }

            return result;
        } catch (TddlException e) {
            throw new TddlNestableRuntimeException(e);
        } finally {
            if (rc != null) {
                List<TddlException> exs = new ArrayList();
                exs = rc.close(exs);
                if (!GeneralUtil.isEmpty(exs)) {
                    for (TddlException e : exs) {
                        logger.warn("close subquery failed, exception is:", e);
                    }
                }
            }
        }

    }
}
