package com.taobao.tddl.repo.mysql.handler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ArrayResultCursor;
import com.taobao.tddl.executor.handler.ExplainHandler;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.mysql.sqlconvertor.MysqlPlanVisitorImpl;
import com.taobao.tddl.repo.mysql.sqlconvertor.SqlAndParam;

/**
 * mysql explain
 * 
 * @author jianghang 2014-6-24 下午1:59:39
 * @since 5.1.0
 */
public class ExplainMyHandler extends ExplainHandler {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        if (executor instanceof IMerge) {
            boolean allCondensable = true;
            for (IDataNodeExecutor dne : ((IMerge) executor).getSubNodes()) {
                allCondensable &= isCondensable(dne, null);
                if (!allCondensable) {
                    break;
                }
            }

            if (allCondensable) {
                return buildCondensableCursor(((IMerge) executor).getSubNodes(), executionContext);
            }
        } else if (isCondensable(executor, null)) {
            return buildCondensableCursor(Arrays.asList(executor), executionContext);
        }

        return super.handle(executor, executionContext);
    }

    private ISchematicCursor buildCondensableCursor(List<IDataNodeExecutor> executors, ExecutionContext executionContext) {
        ArrayResultCursor result = new ArrayResultCursor("explain", executionContext);
        result.addColumn("GROUP_NAME", DataType.StringType);
        result.addColumn("SQL", DataType.StringType);
        result.addColumn("PARAMS", DataType.StringType);
        result.initMeta();

        for (IDataNodeExecutor executor : executors) {
            SqlAndParam sqlAndParam = new SqlAndParam();
            // 构造一下sql
            if (executor.getSql() != null) {
                sqlAndParam.sql = executor.getSql();
                if (executionContext.getParams() != null) {
                    sqlAndParam.param = executionContext.getParams().getCurrentParameter();
                } else {
                    sqlAndParam.param = new HashMap<Integer, ParameterContext>();
                }
            } else {
                if (executor instanceof IQueryTree) {
                    ((IQueryTree) executor).setTopQuery(true);
                    MysqlPlanVisitorImpl visitor = new MysqlPlanVisitorImpl(executor, null, null, null, null, true);
                    executor.accept(visitor);
                    sqlAndParam.sql = visitor.getString();
                    sqlAndParam.param = visitor.getOutPutParamMap();
                } else {
                    MysqlPlanVisitorImpl visitor = new MysqlPlanVisitorImpl(executor,
                        executionContext.getParams() != null ? executionContext.getParams().getFirstParameter() : null,
                        null,
                        null,
                        null,
                        true);
                    executor.accept(visitor);
                    sqlAndParam.sql = visitor.getString();
                    sqlAndParam.param = visitor.getOutPutParamMap();
                    sqlAndParam.newParamIndexToOld = visitor.getNewParamIndexToOldMap();
                }
            }

            result.addRow(new Object[] { executor.getDataNode(), sqlAndParam.sql, sqlAndParam.param });
        }

        return result;
    }

    /**
     * 为ob搞的，ob不支持bigdecimal，统一转double
     * 
     * @param params
     */
    protected void convertBigDecimal(Map<Integer, ParameterContext> params) {
        return;
    }

    private boolean isCondensable(IDataNodeExecutor executor, String lastGroupNode) {
        if (executor instanceof IQuery) {
            IQuery iq = (IQuery) executor;
            IQueryTree iqc = iq.getSubQuery();
            if (iqc == null) {
                return true;
            }
            String groupNode1 = lastGroupNode;
            if (groupNode1 == null) {
                groupNode1 = iq.getDataNode();
            }
            String groupNode2 = iqc.getDataNode();
            if (TStringUtil.equals(groupNode1, groupNode2)) {
                return isCondensable(iqc, groupNode1);
            } else {
                return false;
            }
        } else if (executor instanceof IJoin) {
            IJoin ijoin = (IJoin) executor;
            String leftNode = ijoin.getLeftNode().getDataNode();
            String rightNode = ijoin.getRightNode().getDataNode();
            if (leftNode == null || rightNode == null) {
                return false;
            } else if (!leftNode.equals(rightNode)) {
                return false;
            }

            if (ijoin.getLeftNode() instanceof IMerge || ijoin.getRightNode() instanceof IMerge) {
                return false;
            }

            boolean leftJoin = true;
            boolean rightJoin = true;
            if (ijoin.getLeftNode() instanceof IJoin) {
                leftJoin = isCondensable(ijoin.getLeftNode(), leftNode);
            }
            if (ijoin.getRightNode() instanceof IJoin) {
                rightJoin = isCondensable(ijoin.getRightNode(), rightNode);
            }

            return leftJoin & rightJoin;
        } else if (executor instanceof IPut) {
            // 目前put操作一定是单库的
            return true;
        } else {
            return false;
        }
    }

}
