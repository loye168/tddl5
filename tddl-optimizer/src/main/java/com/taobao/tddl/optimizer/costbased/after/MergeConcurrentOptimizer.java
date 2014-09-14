package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.properties.ConnectionParams;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.common.properties.ParamManager;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.IQueryTree.QUERY_CONCURRENCY;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 判断merge的执行模式：串行，库间并行，全表并行
 * 
 * <pre>
 * 1. 无聚合函数，无条件  串行
 * 2. 不满足1
 *     a. 存在orderBy(包括因为groupBy/distinct/sortMergeJoin的join列下推的orderby)， 生成全表并行
 *     b. 存在groupby或者distinct为所有分库键，并且不存在其余orderby(先做下推)，生成group并行
 *     c. 存在groupBy(已经排除了a的情况)，生成全表并行
 *     d. 其余case，生成group并行
 *     
 * 全表并行优化：如果开启了临时表选择模式，则优先使用group并行来控制连接数的使用
 * </pre>
 * 
 * @author Whisper
 */
public class MergeConcurrentOptimizer implements QueryPlanOptimizer {

    public MergeConcurrentOptimizer(){
    }

    /**
     * 如果设置了MergeConcurrent 并且值为True，则将所有的Merge变为并行
     */
    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Parameters parameterSettings, Map<String, Object> extraCmd) {
        this.findMergeAndSetConcurrent(dne, extraCmd);
        return dne;
    }

    private void findMergeAndSetConcurrent(IDataNodeExecutor dne, Map<String, Object> extraCmd) {
        if (dne instanceof IMerge) {
            QUERY_CONCURRENCY concurrency = judgeMergeConcurrent(extraCmd, (IMerge) dne);
            ((IMerge) dne).setQueryConcurrency(concurrency);

            for (IDataNodeExecutor child : ((IMerge) dne).getSubNodes()) {
                this.findMergeAndSetConcurrent(child, extraCmd);
            }
        }

        if (dne instanceof IJoin) {
            this.findMergeAndSetConcurrent(((IJoin) dne).getLeftNode(), extraCmd);
            this.findMergeAndSetConcurrent(((IJoin) dne).getRightNode(), extraCmd);
        }

        if (dne instanceof IQuery && ((IQuery) dne).getSubQuery() != null) {
            this.findMergeAndSetConcurrent(((IQuery) dne).getSubQuery(), extraCmd);

        }
    }

    /**
     * <pre>
     * 1. 无聚合函数，无条件  串行
     * 2. 不满足1
     *     a. 存在orderBy(包括因为groupBy/distinct/sortMergeJoin的join列下推的orderby)， 生成全部并行
     *     b. 存在groupby或者distinct为所有分库键，并且不存在其余orderby(先做下推)，生成group并行
     *     c. 存在groupBy(已经排除了a的情况)，生成全部并行
     *     d. 其余case，生成group并行
     * </pre>
     */
    private QUERY_CONCURRENCY judgeMergeConcurrent(Map<String, Object> extraCmd, IMerge query) {
        String value = GeneralUtil.getExtraCmdString(extraCmd, ConnectionProperties.MERGE_CONCURRENT);
        boolean isctt = isChooseTemporaryTable(extraCmd);
        if (StringUtils.isEmpty(value)) {
            if (query.getSql() != null) {
                // 如果存在sql，那说明是hint直接路由
                return QUERY_CONCURRENCY.GROUP_CONCURRENT;
            } else if (!query.isExistAggregate()) {
                // 如果不存在聚合计算
                if ((query.getLimitFrom() != null || query.getLimitTo() != null)) {
                    if (query.getOrderBys() == null || query.getOrderBys().isEmpty()) {
                        // 存在limit，但不存在order by时不允许走并行
                        setLayLoad(query, true);
                        return QUERY_CONCURRENCY.SEQUENTIAL;
                    }
                } else if ((query.getOrderBys() == null || query.getOrderBys().isEmpty())
                           && (query.getGroupBys() == null || query.getGroupBys().isEmpty())
                           && query.getHavingFilter() == null) {
                    if (OptimizerUtils.isNoFilter(query)) {
                        // 不存在聚合函数
                        // 没有其他的order by / group by / having /
                        // where等条件时，就是个简单的select *
                        // from xxx，暂时也不做并行
                        setLayLoad(query, true);
                        return QUERY_CONCURRENCY.SEQUENTIAL;
                    }
                }

                if (isctt) {
                    // 开启临时表模式，使用库并行模式
                    return QUERY_CONCURRENCY.GROUP_CONCURRENT;
                }

                // 其余情况，选择group并行
                if (query.getOrderBys() != null && !query.getOrderBys().isEmpty()) {
                    return QUERY_CONCURRENCY.CONCURRENT;
                } else {
                    return QUERY_CONCURRENCY.GROUP_CONCURRENT;
                }
            } else {
                if (isctt) {
                    // 开启临时表模式，使用库并行模式
                    return QUERY_CONCURRENCY.GROUP_CONCURRENT;
                }

                // 存在聚合计算
                if (query.getOrderBys() != null && !query.getOrderBys().isEmpty()) {
                    // case a. 存在orderby
                    return QUERY_CONCURRENCY.CONCURRENT;
                } else if (query.isGroupByShardColumns() || query.isDistinctByShardColumns()) {
                    // case b
                    return QUERY_CONCURRENCY.GROUP_CONCURRENT;
                } else if (query.getGroupBys() != null && !query.getGroupBys().isEmpty()) {
                    // case c
                    return QUERY_CONCURRENCY.CONCURRENT;
                } else {
                    // case d
                    return QUERY_CONCURRENCY.GROUP_CONCURRENT;
                }
            }
        } else {
            if (BooleanUtils.toBoolean(value)) {
                if (isctt) {
                    // 开启临时表模式，使用库并行模式
                    return QUERY_CONCURRENCY.GROUP_CONCURRENT;
                } else {
                    return QUERY_CONCURRENCY.CONCURRENT;
                }
            } else {
                return QUERY_CONCURRENCY.SEQUENTIAL;
            }
        }
    }

    private void setLayLoad(IDataNodeExecutor dne, boolean lazyLoad) {
        if (dne instanceof IQueryTree) {
            dne.setLazyLoad(lazyLoad);
            if (dne instanceof IMerge) {
                for (IDataNodeExecutor child : ((IMerge) dne).getSubNodes()) {
                    setLayLoad(child, lazyLoad);
                }
            }

            if (dne instanceof IJoin) {
                this.setLayLoad(((IJoin) dne).getLeftNode(), lazyLoad);
                this.setLayLoad(((IJoin) dne).getRightNode(), lazyLoad);
            }

            if (dne instanceof IQuery && ((IQuery) dne).getSubQuery() != null) {
                this.setLayLoad(((IQuery) dne).getSubQuery(), lazyLoad);
            }
        } else {
            dne.setLazyLoad(lazyLoad);
        }
    }

    private boolean isChooseTemporaryTable(Map<String, Object> extraCmds) {
        if (extraCmds == null) {
            return false;
        }

        ParamManager paramManager = new ParamManager(extraCmds);
        return paramManager.getBoolean(ConnectionParams.ALLOW_TEMPORARY_TABLE)
               && paramManager.getBoolean(ConnectionParams.CHOOSE_TEMPORARY_TABLE);
    }
}
