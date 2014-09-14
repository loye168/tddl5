package com.taobao.tddl.optimizer.costbased.after;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * 如果设置了MergeConcurrent 并且值为True，则将所有的Merge变为并行
 * 
 * <pre>
 * TODO: 需要考虑 merge join query展开为 merge join merge后，对应的join的返回列，以及对应的filter需要重新build，应该基于语法树来做
 * </pre>
 * 
 * @since 5.0.0
 */
public class MergeJoinMergeOptimizer implements QueryPlanOptimizer {

    /**
     * 如果设置了MergeConcurrent 并且值为True，则将所有的Merge变为并行
     */
    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Parameters parameterSettings, Map<String, Object> extraCmd) {
        return this.findEveryJoin(dne, true, true, extraCmd);
    }

    private IDataNodeExecutor findEveryJoin(IDataNodeExecutor dne, boolean isExpandLeft, boolean isExpandRight,
                                            Map<String, Object> extraCmd) {
        if (dne instanceof IMerge) {
            List<IDataNodeExecutor> subs = ((IMerge) dne).getSubNodes();
            for (int i = 0; i < subs.size(); i++) {
                subs.set(i, this.findEveryJoin(subs.get(i), isExpandLeft, isExpandRight, extraCmd));
            }

            ((IMerge) dne).setSubNodes(subs);
            return dne;
        } else if (dne instanceof IQuery) {
            return dne;
        } else if (dne instanceof IJoin) {
            ((IJoin) dne).setLeftNode((IQueryTree) this.findEveryJoin(((IJoin) dne).getLeftNode(),
                isExpandLeft,
                isExpandRight,
                extraCmd));
            ((IJoin) dne).setRightNode((IQueryTree) this.findEveryJoin(((IJoin) dne).getRightNode(),
                isExpandLeft,
                isExpandRight,
                extraCmd));
            return this.processJoin((IJoin) dne, isExpandLeft, isExpandRight, extraCmd);
        }

        return dne;
    }

    private IQueryTree processJoin(IJoin j, boolean isExpandLeft, boolean isExpandRight, Map<String, Object> extraCmd) {
        // 如果一个节点包含limit，group by，order by等条件，则不能展开
        if (!canExpand(j)) {
            // join节点可能自己存在limit
            isExpandLeft = false;
            isExpandRight = false;
        } else if (!canExpand(j.getLeftNode())) {
            isExpandLeft = false;
        } else if (!canExpand(j.getRightNode())) {
            isExpandRight = false;
        }

        if (isExpandLeft && isExpandRight) {
            return this.cartesianProduct(j, extraCmd);
        } else if (isExpandLeft) {
            return this.expandLeft(j, extraCmd);
        } else if (isExpandRight) {
            return this.expandRight(j, extraCmd);
        } else {
            return j;
        }
    }

    private boolean canExpand(IQueryTree query) {
        // 如果一个节点包含limit，group by，order by等条件
        if (query.getLimitFrom() != null || query.getLimitTo() != null || query.isExistAggregate()) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * 将左边的merge展开，依次和右边做join
     */
    public IQueryTree expandLeft(IJoin j, Map<String, Object> extraCmd) {
        if (!(j.getLeftNode() instanceof IMerge)) {
            return j;
        }

        IMerge left = (IMerge) j.getLeftNode();
        if (!isNeedExpand(left, j.getRightNode(), extraCmd)) {
            return j;
        }

        IMerge newMerge = ASTNodeFactory.getInstance().createMerge();
        for (IDataNodeExecutor leftChild : left.getSubNodes()) {
            IJoin newJoin = (IJoin) j.copy();
            newJoin.setLeftNode((IQueryTree) leftChild);
            newJoin.setRightNode(j.getRightNode());
            newJoin.executeOn(j.getDataNode());
            newMerge.addSubNode(newJoin);
        }

        newMerge.setAlias(j.getAlias());
        newMerge.setColumns(j.getColumns());
        newMerge.setConsistent(j.getConsistent());
        newMerge.setGroupBys(j.getGroupBys());
        newMerge.setLimitFrom(j.getLimitFrom());
        newMerge.setLimitTo(j.getLimitTo());
        newMerge.setOrderBys(j.getOrderBys());
        newMerge.setQueryConcurrency(j.getQueryConcurrency());
        newMerge.having(j.getHavingFilter());
        newMerge.setValueFilter(j.getValueFilter());
        newMerge.setOtherJoinOnFilter(j.getOtherJoinOnFilter());
        newMerge.executeOn(j.getDataNode());
        newMerge.setExistAggregate(j.isExistAggregate());
        newMerge.setIsSubQuery(j.isSubQuery());
        return newMerge;
    }

    /**
     * 将右边的merge展开，依次和左边做join
     * 
     * @param j
     * @return
     */
    public IQueryTree expandRight(IJoin j, Map<String, Object> extraCmd) {
        if (!(j.getRightNode() instanceof IMerge)) {
            return j;
        }

        IMerge right = (IMerge) j.getRightNode();
        if (!isNeedExpand(right, j.getLeftNode(), extraCmd)) {
            return j;
        }

        IMerge newMerge = ASTNodeFactory.getInstance().createMerge();
        for (IDataNodeExecutor rightChild : right.getSubNodes()) {
            IJoin newJoin = (IJoin) j.copy();
            newJoin.setLeftNode(j.getLeftNode());
            ((IQueryTree) rightChild).setAlias(right.getAlias());
            newJoin.setRightNode((IQueryTree) rightChild);
            newJoin.executeOn(j.getDataNode());
            newMerge.addSubNode(newJoin);
        }

        newMerge.setAlias(j.getAlias());
        newMerge.setColumns(j.getColumns());
        newMerge.setConsistent(j.getConsistent());
        newMerge.setGroupBys(j.getGroupBys());
        newMerge.having(j.getHavingFilter());
        newMerge.setLimitFrom(j.getLimitFrom());
        newMerge.setLimitTo(j.getLimitTo());
        newMerge.setOrderBys(j.getOrderBys());
        newMerge.setQueryConcurrency(j.getQueryConcurrency());
        newMerge.setValueFilter(j.getValueFilter());
        newMerge.executeOn(j.getDataNode());
        newMerge.setOtherJoinOnFilter(j.getOtherJoinOnFilter());
        newMerge.setExistAggregate(j.isExistAggregate());
        newMerge.setIsSubQuery(j.isSubQuery());
        return newMerge;
    }

    /**
     * 左右都展开做笛卡尔积
     * 
     * @param j
     * @return
     */
    public IQueryTree cartesianProduct(IJoin j, Map<String, Object> extraCmd) {
        if (j.getLeftNode() instanceof IMerge && !(j.getRightNode() instanceof IMerge)) {
            return this.expandLeft(j, extraCmd);
        }

        if (!(j.getLeftNode() instanceof IMerge) && (j.getRightNode() instanceof IMerge)) {
            return this.expandRight(j, extraCmd);
        }

        if (!(j.getLeftNode() instanceof IMerge) && !(j.getRightNode() instanceof IMerge)) {
            return j;
        }

        if (!GeneralUtil.getExtraCmdBoolean(extraCmd, ConnectionProperties.MERGE_EXPAND, false)) {
            return j;
        }

        IMerge leftMerge = (IMerge) j.getLeftNode();
        IMerge rightMerge = (IMerge) j.getRightNode();
        IMerge newMerge = ASTNodeFactory.getInstance().createMerge();

        for (IDataNodeExecutor leftChild : leftMerge.getSubNodes()) {
            for (IDataNodeExecutor rightChild : rightMerge.getSubNodes()) {
                IJoin newJoin = (IJoin) j.copy();
                newJoin.setLeftNode((IQueryTree) leftChild);
                newJoin.setRightNode((IQueryTree) rightChild);
                newJoin.executeOn(leftChild.getDataNode());
                newMerge.addSubNode(newJoin);
            }
        }
        newMerge.setAlias(j.getAlias());
        newMerge.setColumns(j.getColumns());
        newMerge.setConsistent(j.getConsistent());
        newMerge.setGroupBys(j.getGroupBys());
        newMerge.having(j.getHavingFilter());
        newMerge.setLimitFrom(j.getLimitFrom());
        newMerge.setLimitTo(j.getLimitTo());
        newMerge.setOrderBys(j.getOrderBys());
        newMerge.setQueryConcurrency(j.getQueryConcurrency());
        newMerge.setValueFilter(j.getValueFilter());
        newMerge.executeOn(j.getDataNode());
        newMerge.setOtherJoinOnFilter(j.getOtherJoinOnFilter());
        newMerge.setExistAggregate(j.isExistAggregate());
        newMerge.setIsSubQuery(j.isSubQuery());
        return newMerge;
    }

    /**
     * 左右表是否为单库上的多表 join 单库上的单表/多表
     */
    private static boolean isNeedExpand(IMerge merge, IQueryTree query, Map<String, Object> extraCmd) {
        boolean expand = true;
        for (IDataNodeExecutor child : merge.getSubNodes()) {
            expand &= StringUtils.equals(child.getDataNode(), query.getDataNode());
            if (!expand) {
                return GeneralUtil.getExtraCmdBoolean(extraCmd, ConnectionProperties.MERGE_EXPAND, false);
            }
        }

        return GeneralUtil.getExtraCmdBoolean(extraCmd, ConnectionProperties.MERGE_EXPAND, true);
    }
}
