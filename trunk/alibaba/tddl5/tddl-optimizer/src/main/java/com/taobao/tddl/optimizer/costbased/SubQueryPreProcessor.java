package com.taobao.tddl.optimizer.costbased;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ObjectUtils;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IGroupFilter;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.optimizer.exception.OptimizerException;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.UniqIdGen;

/**
 * 预先处理子查询
 * 
 * <pre>
 * 1. 查找所有filter/select中的子查询
 * 2. 使用结算结果替换filter/select中的子查询
 * 3. 转移子查询到subqueryFilter中,执行计划按照resultFilter单独处理,避免下推
 * </pre>
 * 
 * @author jianghang 2013-12-19 下午6:44:55
 * @since 5.0.0
 */
public class SubQueryPreProcessor {

    public static QueryTreeNode opitmize(QueryTreeNode qtn) throws OptimizerException {
        if (qtn.getWhereFilter() != null) {
            SubqueryFilterOptimizeResult result = optimizeFilter(qtn.getWhereFilter());
            if (result.subqueryFilter != null) {
                qtn.query(result.filter);
                qtn.setAllWhereFilter(result.filter); // 也清理下all whereFilter
                qtn.setSubqueryFilter(result.subqueryFilter);
            }
        }

        List<IFunction> func = new ArrayList<IFunction>();
        findQuery(qtn, func, null, true);
        if (func.size() > 0) {
            throw new OptimizerException("暂不支持非where条件的correlate subquery");
        }
        return qtn;
    }

    public static IFunction findNextSubqueryOnFilter(QueryTreeNode qtn) {
        return findNextSubqueryOnFilter(qtn, false);
    }

    /**
     * 查找filter需要处理的下一个subquery (采取了深度优先遍历,链表中第一个为叶子节点)
     */
    public static IFunction findNextSubqueryOnFilter(QueryTreeNode qtn, boolean existCorrelated) {
        if (qtn == null) {
            return null;
        }

        List<IFunction> func = findAllSubqueryOnFilter(qtn, existCorrelated);
        if (func.size() > 0) {
            return func.get(0);
        } else {
            return null;
        }
    }

    public static List<IFunction> findAllSubqueryOnFilter(QueryTreeNode qtn) throws OptimizerException {
        return findAllSubqueryOnFilter(qtn, false);
    }

    /**
     * 查找filter中所有的subquery
     */
    public static List<IFunction> findAllSubqueryOnFilter(QueryTreeNode qtn, boolean existCorrelated)
                                                                                                     throws OptimizerException {
        List<IFunction> func = new ArrayList<IFunction>();
        if (qtn == null) {
            return func;
        }

        findQuery(qtn, func, null, existCorrelated);
        findFilter(qtn.getSubqueryFilter(), func, null, existCorrelated);
        return func;
    }

    /**
     * 根据执行计划的计算结果，替换subquery
     */
    public static IFunction assignmentSubqueryOnFilter(QueryTreeNode qtn, Map<Long, Object> subquerySettings)
                                                                                                             throws OptimizerException {
        List<IFunction> func = new ArrayList<IFunction>();
        findQuery(qtn, func, subquerySettings, false);
        findFilter(qtn.getSubqueryFilter(), func, subquerySettings, false);
        if (func.size() > 0) {
            return func.get(0);
        } else {
            return null;
        }
    }

    public static class SubqueryFilterOptimizeResult {

        IFilter filter;
        IFilter subqueryFilter;
    }

    private static SubqueryFilterOptimizeResult optimizeFilter(IFilter filter) {
        SubqueryFilterOptimizeResult result = new SubqueryFilterOptimizeResult();
        List<IFunction> func = new ArrayList<IFunction>();
        findFilter(filter, func, null, true);
        if (func.size() == 0) {// 不存在子查询
            result.filter = filter;
            return result;
        }

        if (!FilterUtils.isCNFNode(filter)) { // 存在or关系
            result.subqueryFilter = filter;
            result.filter = null;
            return result;
        } else {
            List<IFilter> filters = FilterUtils.toDNFNode(filter);
            List<IFilter> newFilters = new ArrayList<IFilter>();
            IFilter subqueryFilter = null;
            for (IFilter f : filters) {
                func.clear();
                findFilter(f, func, null, true);
                if (func.size() > 0) { // 存在子查询
                    subqueryFilter = FilterUtils.and(subqueryFilter, f);
                } else {
                    newFilters.add(f);// 保留在老filter中
                }
            }

            result.subqueryFilter = subqueryFilter;
            result.filter = FilterUtils.DNFToAndLogicTree(newFilters);
            return result;
        }

    }

    private static void findQuery(QueryTreeNode qtn, List<IFunction> func, Map<Long, Object> subquerySettings,
                                  boolean existCorrelated) {
        findFilter(qtn.getKeyFilter(), func, subquerySettings, existCorrelated);
        findFilter(qtn.getWhereFilter(), func, subquerySettings, existCorrelated);
        findFilter(qtn.getResultFilter(), func, subquerySettings, existCorrelated);
        findFilter(qtn.getOtherJoinOnFilter(), func, subquerySettings, existCorrelated);
        findFilter(qtn.getHavingFilter(), func, subquerySettings, existCorrelated);

        List<ISelectable> newSelect = new ArrayList<ISelectable>();
        for (ISelectable select : qtn.getColumnsSelected()) {
            // 可能替换了subquery
            Object s = findSelectable(select, func, subquerySettings, existCorrelated);
            if (s != null) {
                if (s instanceof ISelectable) {
                    newSelect.add((ISelectable) s);
                } else {
                    newSelect.add(buildConstanctFilter(s, ((ISelectable) select).getAlias()));
                }
            } else {
                newSelect.add(select);
            }
        }
        qtn.select(newSelect);

        if (qtn.getOrderBys() != null) {
            for (IOrderBy orderBy : qtn.getOrderBys()) {
                findOrderBy(orderBy, func, subquerySettings, existCorrelated);
            }
        }

        if (qtn.getGroupBys() != null) {
            for (IOrderBy groupBy : qtn.getGroupBys()) {
                findOrderBy(groupBy, func, subquerySettings, existCorrelated);
            }
        }
    }

    private static void findFilter(IFilter filter, List<IFunction> func, Map<Long, Object> subquerySettings,
                                   boolean existCorrelated) {
        if (filter == null) {
            return;
        }

        if (filter instanceof ILogicalFilter) {
            for (IFilter sub : ((ILogicalFilter) filter).getSubFilter()) {
                findFilter(sub, func, subquerySettings, existCorrelated);
            }
        } else if (filter instanceof IGroupFilter) {
            for (IFilter sub : ((IGroupFilter) filter).getSubFilter()) {
                findFilter(sub, func, subquerySettings, existCorrelated);
            }
        } else {
            findBooleanFilter((IBooleanFilter) filter, func, subquerySettings, existCorrelated);
        }

    }

    private static void findBooleanFilter(IBooleanFilter filter, List<IFunction> func,
                                          Map<Long, Object> subquerySettings, boolean existCorrelated) {
        if (filter == null) {
            return;
        }

        Object column = filter.getColumn();
        Object value = filter.getValue();

        if (column instanceof IFunction && isSubqueryFunction((IFunction) column)) {
            if (((IFunction) column).getArgs().get(0) instanceof QueryTreeNode) {
                QueryTreeNode subquery = (QueryTreeNode) ((IFunction) column).getArgs().get(0);
                // 深度优先,尝试递归找一下
                findQuery(subquery, func, subquerySettings, existCorrelated);
                Object obj = getSubqueryValue(subquerySettings, subquery);
                // 可能已经计算出了结果，替换一下
                if (obj != null && !(obj instanceof QueryTreeNode)) {
                    filter.setColumn(obj);
                } else {
                    addSubqueryOnFilter(func, (IFunction) column, existCorrelated);
                }
            }
        } else if (column instanceof ISelectable) {
            Object select = findSelectable((ISelectable) column, func, subquerySettings, existCorrelated);
            if (select != null) {
                filter.setColumn(select);
            }
        }

        // subQuery，比如WHERE ID = (SELECT ID FROM A)
        if (value instanceof IFunction && isSubqueryFunction((IFunction) value)) {
            if (((IFunction) value).getArgs().get(0) instanceof QueryTreeNode) {
                QueryTreeNode subquery = (QueryTreeNode) ((IFunction) value).getArgs().get(0);
                // 深度优先,尝试递归找一下
                findQuery(subquery, func, subquerySettings, existCorrelated);
                Object obj = getSubqueryValue(subquerySettings, subquery);
                // 可能已经计算出了结果，替换一下
                if (obj != null && !(obj instanceof QueryTreeNode)) {
                    filter.setValue(obj);
                } else {
                    addSubqueryOnFilter(func, (IFunction) value, existCorrelated);
                }
            }
        } else if (value instanceof ISelectable) {
            Object select = findSelectable((ISelectable) value, func, subquerySettings, existCorrelated);
            if (select != null) {
                filter.setValue(select);
            }
        }

        if (filter.getOperation() == OPERATION.IN) {
            List<Object> values = filter.getValues();
            if (values != null && !values.isEmpty()) {
                // in的子查询
                if (values.get(0) instanceof IFunction && isSubqueryFunction((IFunction) values.get(0))) {
                    if (((IFunction) values.get(0)).getArgs().get(0) instanceof QueryTreeNode) {
                        QueryTreeNode subquery = (QueryTreeNode) ((IFunction) values.get(0)).getArgs().get(0);
                        // 深度优先,尝试递归找一下
                        findQuery(subquery, func, subquerySettings, existCorrelated);
                        Object obj = getSubqueryValue(subquerySettings, subquery);
                        // 可能已经计算出了结果，替换一下
                        if (obj != null && !(obj instanceof QueryTreeNode)) {
                            filter.setValues((List<Object>) obj); // 一定会是list,否则就是执行器的bug
                        } else {
                            addSubqueryOnFilter(func, (IFunction) values.get(0), existCorrelated);
                        }
                    }
                }
            }
        }

    }

    private static Object findSelectable(ISelectable select, List<IFunction> func, Map<Long, Object> subquerySettings,
                                         boolean existCorrelated) {
        if (select instanceof IFilter) {
            findFilter((IFilter) select, func, subquerySettings, existCorrelated);
        } else if (select instanceof IFunction) {
            return findFunction((IFunction) select, func, subquerySettings, existCorrelated);
        } else if (select instanceof IColumn) {
            return findColumn((IColumn) select, func, subquerySettings, existCorrelated);
        }

        return null;
    }

    private static Object findColumn(IColumn column, List<IFunction> func, Map<Long, Object> subquerySettings,
                                     boolean existCorrelated) {
        if (column.getCorrelateOnFilterId() > 0L) {
            if (subquerySettings == null) {
                return null;
            }

            Object obj = subquerySettings.get(column.getCorrelateOnFilterId());
            return obj;
        }

        return null;
    }

    private static void findOrderBy(IOrderBy order, List<IFunction> func, Map<Long, Object> subquerySettings,
                                    boolean existCorrelated) {
        if (order.getColumn() instanceof ISelectable) {
            Object select = findSelectable(order.getColumn(), func, subquerySettings, existCorrelated);
            if (select != null) {
                if (!(select instanceof ISelectable)) {
                    order.setColumn(buildConstanctFilter(select, ((ISelectable) order.getColumn()).getAlias()));
                } else {
                    order.setColumn((ISelectable) select);
                }
            }
        }
    }

    private static Object findFunction(IFunction f, List<IFunction> func, Map<Long, Object> subquerySettings,
                                       boolean existCorrelated) {
        for (Object arg : f.getArgs()) {
            if (arg instanceof ISelectable) {
                Object obj = findSelectable((ISelectable) arg, func, subquerySettings, existCorrelated);
                if (obj != null) {
                    return obj;
                }
            } else if (arg instanceof QueryTreeNode) { // scalar subquery
                // 深度优先,尝试递归找一下
                findQuery((QueryTreeNode) arg, func, subquerySettings, existCorrelated);
                Object obj = getSubqueryValue(subquerySettings, (QueryTreeNode) arg);
                if (obj != null && !(obj instanceof QueryTreeNode)) {
                    return obj;
                } else {
                    addSubqueryOnFilter(func, f, existCorrelated);
                }
            }
        }

        return null;
    }

    private static boolean isSubqueryFunction(IFunction func) {
        return func.getFunctionName().equals(IFunction.BuiltInFunction.SUBQUERY_LIST)
               || func.getFunctionName().equals(IFunction.BuiltInFunction.SUBQUERY_SCALAR);
    }

    private static Object getSubqueryValue(Map<Long, Object> subquerySettings, QueryTreeNode subquery) {
        if (subquerySettings == null) {
            return null;
        }

        if (subquerySettings.containsKey(subquery.getSubqueryOnFilterId())) {
            Object obj = subquerySettings.get(subquery.getSubqueryOnFilterId());
            if (obj == null) {
                return NullValue.getNullValue();
            }

            return obj;
        }

        return null;
    }

    private static void addSubqueryOnFilter(List<IFunction> func, IFunction f, boolean existCorrelated) {
        if (func == null) {
            return;
        }

        boolean exist = false;
        QueryTreeNode query = (QueryTreeNode) f.getArgs().get(0);
        Long subqueryOnFilterId = query.getSubqueryOnFilterId();
        if (subqueryOnFilterId == null || subqueryOnFilterId == 0) {
            query.setSubqueryOnFilterId(UniqIdGen.genSubqueryID());
        } else {
            // 可能whereFilter和resultFilter中有重复的filter
            for (IFunction fc : func) {
                if (((QueryTreeNode) fc.getArgs().get(0)).getSubqueryOnFilterId().equals(subqueryOnFilterId)) {
                    exist = true;
                    break;
                }
            }

        }

        if (!exist) {
            func.add(f);
        }

        if (!existCorrelated) {
            // 清理下correlated
            processCorrelated(func);
        }
    }

    /**
     * 处理下叶子节点是否存在correlated 查询
     */
    private static boolean processCorrelated(List<IFunction> func) {
        if (func.size() > 0) {
            QueryTreeNode query = (QueryTreeNode) func.get(0).getArgs().get(0);
            if (query.isCorrelatedSubquery()) {
                func.clear();
                return true;
            }
        }

        return false;
    }

    private static IBooleanFilter buildConstanctFilter(Object constant, String alias) {
        IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
        f.setOperation(OPERATION.CONSTANT);
        f.setColumn(constant);
        f.setColumnName(ObjectUtils.toString(constant));
        f.setAlias(alias);
        return f;
    }
}
