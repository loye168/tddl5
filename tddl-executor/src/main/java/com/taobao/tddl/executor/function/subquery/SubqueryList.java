package com.taobao.tddl.executor.function.subquery;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;

/**
 * @since 5.0.0
 */
public class SubqueryList extends AbstractSubQueryFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        IQueryTree plan = this.getQueryPlan(args);
        ISchematicCursor rc = null;
        try {
            rc = ExecutorContext.getContext().getTopologyExecutor().execByExecPlanNode(plan, ec);
            IRowSet row = null;
            List<Object> res = new ArrayList();
            while ((row = rc.next()) != null) {
                ColumnMeta cm = row.getParentCursorMeta().getColumns().get(0);
                Object value = ExecUtils.getValueByColumnMeta(row, cm);
                res.add(value);
            }
            return res;
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

    @Override
    public String[] getFunctionNames() {
        return new String[] { IFunction.BuiltInFunction.SUBQUERY_LIST };
    }
}
