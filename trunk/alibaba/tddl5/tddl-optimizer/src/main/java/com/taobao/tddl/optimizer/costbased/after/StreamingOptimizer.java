package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import org.apache.commons.lang.BooleanUtils;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * streaming模式处理
 * 
 * <pre>
 * 几种情况需要使用streaming
 * 1. 当前执行计划节点，包括子节点在内，不存在where条件，比如select * from xxx. 选择streaming模式
 * 2. 针对merge/join/subquery节点，如果存在limit参数
 *    a. 针对merge节点，因为limit参数可下推，需要判断下limit from+to的参数值是否超过阀值(下推到子节点后是0->from+to)
 *    b. 针对join和subquery，目前未做limit参数下推，所以直接选择streaming模式
 *    c. 针对query节点，无子节点并且父节点不为streaming模式，当前节点不选择streaming.
 * </pre>
 * 
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 */
public class StreamingOptimizer implements QueryPlanOptimizer {

    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Parameters parameterSettings, Map<String, Object> extraCmd) {
        boolean parentStreaming = false;
        String forceStreaming = GeneralUtil.getExtraCmdString(extraCmd, ConnectionProperties.CHOOSE_STREAMING);
        if (forceStreaming != null) {
            boolean force = BooleanUtils.toBoolean(forceStreaming);
            if (force == false) { // 强制关闭，忽略此优化
                return dne;
            }

            parentStreaming = force;
        }

        if (dne instanceof IQueryTree) {
            this.findQueryOptimizeStreaming(dne, parentStreaming, extraCmd);
        }

        return dne;
    }

    /**
     * 递归设置streaming，比如父节点parentStreaming为true时，当前节点包括子节点都应采用streaming模式来保证
     */
    private void findQueryOptimizeStreaming(IDataNodeExecutor dne, boolean parentStreaming, Map<String, Object> extraCmd) {
        if (!(dne instanceof IQueryTree)) {
            return;
        }

        // 当前节点如果无任何条件,并且无limit
        if (isSimpleQuery((IQueryTree) dne)) {
            parentStreaming = true;
        }

        boolean streaming = (parentStreaming || isNeedStreaming((IQueryTree) dne, extraCmd));
        if (dne instanceof IMerge) {
            for (IDataNodeExecutor child : ((IMerge) dne).getSubNodes()) {
                this.findQueryOptimizeStreaming(child, streaming, extraCmd);
            }
        } else if (dne instanceof IJoin) {
            this.findQueryOptimizeStreaming(((IJoin) dne).getLeftNode(), streaming, extraCmd);
            if (((IJoin) dne).getJoinStrategy() == JoinStrategy.SORT_MERGE_JOIN) {
                // 针对sort merge join，需要考虑右表，其余情况为in模式和block模式，不需要理会streaming
                this.findQueryOptimizeStreaming(((IJoin) dne).getRightNode(), streaming, extraCmd);
            }
        } else if (dne instanceof IQuery) {
            if (((IQuery) dne).getSubQuery() != null) {
                this.findQueryOptimizeStreaming(((IQuery) dne).getSubQuery(), streaming, extraCmd);
            }
        }

        // 针对可下推的case，不做单独判断，执行器执行的时候如果发现整个节点可下推，不会理会子节点的streaming属性
        dne.setStreaming(parentStreaming);
    }

    /**
     * 没有聚合操作，没有hint sql，没有条件
     * 
     * @param query
     * @return
     */
    private static boolean isSimpleQuery(IQueryTree query) {
        if (query.getSql() != null) { // 如果是sql单库下推，不管
            return false;
        }

        Comparable from = query.getLimitFrom();
        Comparable to = query.getLimitTo();
        if (from == null && to == null && OptimizerUtils.isNoFilter(query) && !query.isExistAggregate()) {// 不存在limit
            return true;
        } else {
            return false;
        }
    }

    private static boolean isNeedStreaming(IQueryTree query, Map<String, Object> extraCmd) {
        // 存在limit条件
        Comparable from = query.getLimitFrom();
        Comparable to = query.getLimitTo();
        if (from == null && to == null) {// 不存在limit
            return false;
        } else {
            return true;
        }
    }

}
