package com.taobao.tddl.optimizer.costbased.pusher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.exception.OptimizerException;

/**
 * 将merge/join中的order by条件下推，包括隐式的order by条件，比如将groupBy转化为orderBy
 * 
 * <pre>
 * a. 如果orderBy中包含function，也不做下推
 * b. 如果orderBy中的的column字段来自于子节点的函数查询，也不做下推
 * c. 如果join是SortMergeJoin，会下推join列
 * 
 * 比如: tabl1.join(table2).on("table1.id=table2.id").orderBy("id")
 * 转化为：table.orderBy(id).join(table2).on("table1.id=table2.id")
 * 
 * 下推例子：
 * 1. 
 *  父节点：order by c1 ,c2 ,c3
 *  子节点: order by c1, c2
 *  优化：下推c3
 * 
 * 2. 
 *  父节点：order by c2 ,c3  (顺序不一致，强制下推)
 *  子节点: order by c2, c3，无limit
 *  优化：不下推
 *  
 * 3.
 *  父节点：order by c2 ,c3  (顺序不一致，因为子节点存在limit，不可下推order by)
 *  子节点: order by c2, c3，存在limit
 *  优化：不下推
 * 
 * 4. 
 *  父节点：order by c1, c2 ,c3
 *  子节点: 无
 *  优化：下推c1,c2,c3
 * 
 * 5. 
 *  父节点：order by count(*)  (函数不下推)
 *  子节点: 无
 *  优化：不下推
 * 
 * @author jianghang 2013-12-10 下午5:33:16
 * @since 5.0.0
 */
public class OrderByPusher {

    /**
     * 详细优化见类描述 {@linkplain OrderByPusher}
     */
    public static QueryTreeNode optimize(QueryTreeNode qtn) {
        qtn = optimizeDistinct(qtn);
        qtn = pushOrderBy(qtn);
        return qtn;
    }

    /**
     * 处理Merge节点的distinct处理，需要底下节点先做排序
     * 
     * <pre>
     * 排序优先级：
     * groupBy > orderby > distinct
     * </pre>
     */
    private static QueryTreeNode optimizeDistinct(QueryTreeNode qtn) {
        for (ASTNode child : ((QueryTreeNode) qtn).getChildren()) {
            if (child instanceof QueryTreeNode) {
                optimizeDistinct((QueryTreeNode) child);
            }
        }

        if (qtn instanceof MergeNode) {
            MergeNode merge = (MergeNode) qtn;
            if (!(merge.getChild() instanceof QueryTreeNode)) {
                return merge;
            }

            if (containsDistinctColumns(merge)) {
                for (ASTNode con : merge.getChildren()) {
                    QueryTreeNode child = (QueryTreeNode) con;
                    // 重新构建order by / group by字段
                    if (!child.getGroupBys().isEmpty()) {
                        List<IOrderBy> implicitOrderBys = child.getImplicitOrderBys();
                        // 如果order by包含了所有的group by顺序
                        if (isStartWithSameOrderBys(implicitOrderBys, child.getGroupBys())) {
                            child.setOrderBys(implicitOrderBys);
                        } else {
                            // order by不是一个group by的子集，优先使用group by
                            child.setOrderBys(child.getGroupBys());
                        }
                    }

                    // 将查询所有字段进行order by，保证每个child返回的数据顺序都是一致的
                    List<IOrderBy> distinctOrderbys = new ArrayList<IOrderBy>();
                    for (ISelectable s : child.getColumnsSelected()) {
                        if (s instanceof IFunction) {
                            // count(distinct id)不列入distinct字段，因为已有distinct id的列
                            continue;
                        }
                        IOrderBy order = ASTNodeFactory.getInstance().createOrderBy();
                        order.setColumn(s).setDirection(true);
                        distinctOrderbys.add(order);
                    }

                    boolean isDistinctByShardColumns = merge.isDistinctByShardColumns();
                    if (child.getOrderBys() == null || child.getOrderBys().isEmpty()) {
                        // 针对没有业务order by列
                        // 判断一下是否为distinct分库键
                        if (isGroupByShardColumns(distinctOrderbys, child)) {
                            merge.setDistinctByShardColumns(true);
                            isDistinctByShardColumns = true;
                        }
                    }

                    // 尝试去掉function函数，比如count(distinct id)
                    List<IFunction> toRemove = new ArrayList();
                    for (ISelectable s : child.getColumnsSelected()) {
                        if (s instanceof IFunction) {
                            toRemove.add((IFunction) s);
                        }
                    }
                    if (isDistinctByShardColumns) {
                        if (!toRemove.isEmpty()) { // 存在count(distinct id)函数
                            List<ISelectable> distinctColumnRemove = new ArrayList();
                            for (ISelectable s : child.getColumnsSelected()) {
                                if (s instanceof ISelectable && ((ISelectable) s).isDistinct()) {
                                    distinctColumnRemove.add((ISelectable) s);
                                }
                            }
                            // 删除distinct id列
                            child.getColumnsSelected().removeAll(distinctColumnRemove);
                        }
                    } else {
                        child.getColumnsSelected().removeAll(toRemove);

                        // 尝试调整下distinct的order by顺序，调整不了的话，按照distinct columns顺序
                        List<IOrderBy> orderbys = getPushOrderBysCombileOrderbyColumns(distinctOrderbys,
                            child.getOrderBys());
                        if (orderbys.isEmpty()) {
                            child.setOrderBys(distinctOrderbys);
                        } else {
                            child.setOrderBys(orderbys);
                        }
                    }

                    // 清空child的group by，由merge节点进行处理
                    child.setGroupBys(new ArrayList(0));
                }

                if (!merge.isDistinctByShardColumns()) {
                    // merge上的函数设置为distinct标记
                    for (ISelectable s : merge.getColumnsSelected()) {
                        if (s instanceof IFunction && isDistinct(s)) {
                            ((IFunction) s).setNeedDistinctArg(true);
                        }
                    }
                }

                merge.build();
                return merge;
            }
        } else if (qtn instanceof JoinNode || qtn instanceof QueryNode) {
            if (qtn.getGroupBys() != null && !qtn.getGroupBys().isEmpty()) {
                // 如果存在group by + distinct，暂时无法做优化
                return qtn;
            }
            if (containsDistinctColumns(qtn)) {
                // 将查询所有字段进行order by，保证每个child返回的数据顺序都是一致的
                List<IOrderBy> distinctOrderbys = new ArrayList<IOrderBy>();
                for (ISelectable s : qtn.getColumnsSelected()) {
                    IOrderBy order = ASTNodeFactory.getInstance().createOrderBy();
                    order.setColumn(s).setDirection(true);
                    distinctOrderbys.add(order);
                }

                List<IOrderBy> orderbys = getPushOrderBysCombileOrderbyColumns(distinctOrderbys, qtn.getOrderBys());
                if (!orderbys.isEmpty()) {
                    // 尝试合并order by和distinct成功，则设置当前order by
                    qtn.setOrderBys(orderbys);
                }

                qtn.build();
            }
        }

        return qtn;
    }

    private static QueryTreeNode pushOrderBy(QueryTreeNode qtn) {
        if (qtn instanceof MergeNode) {
            MergeNode merge = (MergeNode) qtn;
            if (!(merge.getChild() instanceof QueryTreeNode)) {
                return qtn;
            }

            if (merge.isUnion()) {
                List<IOrderBy> standardOrder = ((QueryTreeNode) merge.getChild()).getImplicitOrderBys();
                for (ASTNode child : merge.getChildren()) {
                    ((QueryTreeNode) child).setOrderBys(new ArrayList(0));

                    // 优先以主键为准
                    if (child instanceof TableNode) {
                        if (((TableNode) child).getIndexUsed().isPrimaryKeyIndex()) {
                            standardOrder = ((TableNode) child).getImplicitOrderBys();
                        }
                    }
                }

                for (ASTNode child : merge.getChildren()) {
                    ((QueryTreeNode) child).setOrderBys(standardOrder);
                    ((QueryTreeNode) child).build();
                }
            } else {
                // 比如merge节点同时存在order by/group by
                // 1. 优先下推父节点的group by到子节点
                // 2. 然后去掉子节点的group by
                for (ASTNode child : merge.getChildren()) {
                    if (!(child instanceof QueryTreeNode)) {
                        continue;
                    }

                    QueryTreeNode qn = (QueryTreeNode) child;
                    if (qn.getGroupBys() != null && !qn.getGroupBys().isEmpty()) {

                        boolean isGroupByShardColumns = false;
                        if ((qn.getOrderBys() == null || qn.getOrderBys().isEmpty())
                            && isGroupByShardColumns(qn.getGroupBys(), qn)) {
                            // 针对无order by,并且是groupBy分库键,打个标记
                            merge.setGroupByShardColumns(true);
                            isGroupByShardColumns = true;
                        } else {
                            // 如果非分库键的group, having在merge做，child清空having条件
                            qn.having("");
                        }

                        if (isGroupByShardColumns) {
                            // 跳过
                        } else {
                            // 需要考虑，如果子节点的列中存在聚合函数，则不能去除子节点的group by，否则语义不正确
                            // order by中可能有desc的倒排语法
                            // 目前的做法是设置orderby/groupby使用相同的列
                            List<IOrderBy> implicitOrderBys = qn.getImplicitOrderBys();
                            // order by不是一个group by的子集，优先使用group by
                            if (isStartWithSameOrderBys(implicitOrderBys, qn.getGroupBys())) {
                                qn.setOrderBys(implicitOrderBys);
                            } else {
                                qn.setOrderBys(qn.getGroupBys());
                            }
                        }
                        qn.build();
                    }
                }

                merge.build();
            }
        } else if (qtn instanceof JoinNode) {
            // index nested loop中的order by，可以推到左节点
            JoinNode join = (JoinNode) qtn;
            if (join.getJoinStrategy() == JoinStrategy.INDEX_NEST_LOOP
                || join.getJoinStrategy() == JoinStrategy.NEST_LOOP_JOIN) {
                // 左表排序隐式下推
                List<IOrderBy> orders = getPushOrderBys(join, join.getImplicitOrderBys(), join.getLeftNode(), true);
                pushJoinOrder(orders, join.getLeftNode(), join.isUedForIndexJoinPK());
            } else if (join.getJoinStrategy() == JoinStrategy.SORT_MERGE_JOIN) {
                // 如果是sort merge join中的order by，需要推到左/右节点
                List<IOrderBy> implicitOrders = join.getImplicitOrderBys();
                Map<ISelectable, IOrderBy> orderColumnsMap = new HashMap<ISelectable, IOrderBy>();
                for (IOrderBy order : implicitOrders) {
                    orderColumnsMap.put(order.getColumn(), order);
                }

                List<IOrderBy> leftJoinColumnOrderbys = new ArrayList<IOrderBy>();
                List<IOrderBy> rightJoinColumnOrderbys = new ArrayList<IOrderBy>();
                for (IBooleanFilter joinFilter : join.getJoinFilter()) {
                    ISelectable column = (ISelectable) joinFilter.getColumn();
                    ISelectable value = (ISelectable) joinFilter.getValue();
                    if (!(column instanceof IColumn && value instanceof IColumn)) {
                        throw new OptimizerException("join列出现函数列,下推函数orderby过于复杂,此时sort merge join无法支持");
                    }

                    // 复制下隐藏orderby的asc/desc
                    boolean asc = true;
                    IOrderBy o = orderColumnsMap.get(column);
                    if (o != null) {
                        asc = o.getDirection();
                    }

                    o = orderColumnsMap.get(value);
                    if (o != null) {
                        asc = o.getDirection();
                    }

                    leftJoinColumnOrderbys.add(ASTNodeFactory.getInstance()
                        .createOrderBy()
                        .setColumn(column)
                        .setDirection(asc));
                    rightJoinColumnOrderbys.add(ASTNodeFactory.getInstance()
                        .createOrderBy()
                        .setColumn(value)
                        .setDirection(asc));
                }
                // 调整下join orderBys的顺序，尽可能和原始的order by顺序一致，这样可以有利于下推
                adjustJoinColumnByImplicitOrders(leftJoinColumnOrderbys, rightJoinColumnOrderbys, implicitOrders);
                // 先推join列，
                // 如果join列推失败了，比如join列是函数列，这问题就蛋疼了，需要提前做判断
                List<IOrderBy> leftOrders = getPushOrderBys(join, leftJoinColumnOrderbys, join.getLeftNode(), true);
                pushJoinOrder(leftOrders, join.getLeftNode(), join.isUedForIndexJoinPK());

                List<IOrderBy> rightOrders = getPushOrderBys(join, rightJoinColumnOrderbys, join.getRightNode(), true);
                pushJoinOrder(rightOrders, join.getRightNode(), join.isUedForIndexJoinPK());
                if (!implicitOrders.isEmpty()) {
                    // group by + order by的隐藏列，如果和join列前缀相同，则下推，否则忽略
                    // 已经推了一次join column，这里不能再强推了
                    leftOrders = getPushOrderBys(join, implicitOrders, join.getLeftNode(), false);
                    rightOrders = getPushOrderBys(join, implicitOrders, join.getRightNode(), false);
                    if (!leftOrders.isEmpty() || !rightOrders.isEmpty()) {
                        pushJoinOrder(leftOrders, join.getLeftNode(), join.isUedForIndexJoinPK());
                        pushJoinOrder(rightOrders, join.getRightNode(), join.isUedForIndexJoinPK());
                    } else {
                        // 尝试一下只推group by的排序，减少一层排序
                        if (join.getGroupBys() != null && !join.getGroupBys().isEmpty()) {
                            List<IOrderBy> leftImplicitOrders = getPushOrderBysCombileOrderbyColumns(join.getGroupBys(),
                                leftJoinColumnOrderbys);
                            List<IOrderBy> rightImplicitOrders = getPushOrderBysCombileOrderbyColumns(join.getGroupBys(),
                                rightJoinColumnOrderbys);

                            // 重置下group by的顺序
                            if (!leftImplicitOrders.isEmpty()) {
                                join.setGroupBys(leftImplicitOrders);
                            } else if (!rightImplicitOrders.isEmpty()) {
                                join.setGroupBys(rightImplicitOrders);
                            }

                            leftOrders = getPushOrderBys(join, leftImplicitOrders, join.getLeftNode(), false);
                            rightOrders = getPushOrderBys(join, rightImplicitOrders, join.getRightNode(), false);
                            if (!leftOrders.isEmpty() || !rightOrders.isEmpty()) {
                                pushJoinOrder(leftOrders, join.getLeftNode(), join.isUedForIndexJoinPK());
                                pushJoinOrder(rightOrders, join.getRightNode(), join.isUedForIndexJoinPK());
                            }
                        }
                    }
                }
            }

            join.build();
        } else if (qtn instanceof QueryNode) {
            // 可以将order推到子查询
            QueryNode query = (QueryNode) qtn;
            List<IOrderBy> orders = getPushOrderBys(query, query.getImplicitOrderBys(), query.getChild(), true);
            if (orders != null && !orders.isEmpty()) {
                for (IOrderBy order : orders) {
                    query.getChild().orderBy(order.getColumn(), order.getDirection());
                }
            }

            query.build();
        }

        for (ASTNode child : ((QueryTreeNode) qtn).getChildren()) {
            if (child instanceof QueryTreeNode) {
                pushOrderBy((QueryTreeNode) child);
            }
        }

        return qtn;
    }

    /**
     * 判断是否存在distinct函数
     */
    private static boolean containsDistinctColumns(QueryTreeNode qc) {
        for (ISelectable c : qc.getColumnsSelected()) {
            if (isDistinct(c)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDistinct(ISelectable s) {
        if (s.isDistinct()) {
            return true;
        }

        if (s instanceof IFunction) {
            for (Object arg : ((IFunction) s).getArgs()) {
                if (arg instanceof ISelectable) {
                    if (isDistinct((ISelectable) arg)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * 判断两个排序列表是否完全相同
     * 
     * <pre>
     * 针对order by id,name group by id
     * 子节点先按照id做group by,因为子节点的orderby和父节点的groupBy满足一个前序匹配，这里直接使用父节点id,Name做order by
     * merge节点先按id做group by(底下子节点为id,name排序，不需要临时表排序)，groupby处理后的结果也满足id,name的排序，直接返回，不需要临时表
     * 
     * 比如：order by id,name group by id , 合并的结果为order by id,name 和 group by id是一个前序匹配
     * 比如：order by id group by name id , 合并的结果为order by id,name，和 group by id,name是一个前序匹配
     * 比如：order by name,id group by id , 合并的结果为order by name,id，和group by id不是一个前序匹配
     * 比如：order by id group by name,school , 合并的结果为order by id，和group by name,school不是一个前序匹配
     * </pre>
     * 
     * @param orderBys
     * @param groupBys
     * @return
     */
    private static boolean isStartWithSameOrderBys(List<IOrderBy> orderBys, List<IOrderBy> groupBys) {
        if (groupBys.size() > orderBys.size()) {
            return false;
        }

        int i = 0;
        for (IOrderBy group : groupBys) {
            boolean found = false;
            int j = 0;
            for (IOrderBy order : orderBys) {
                if (order.getColumn().equals(group.getColumn())) {
                    found = true;
                    break;
                }

                j++;
            }

            if (!found || i != j) {
                // 没找到或者顺序不匹配
                return false;
            }

            i++;
        }
        return true;
    }

    /**
     * 调整join列，按照order by的顺序，有利于下推
     */
    private static void adjustJoinColumnByImplicitOrders(List<IOrderBy> leftJoinOrders, List<IOrderBy> rightJoinOrders,
                                                         List<IOrderBy> implicitOrders) {
        // 调整下join orderBys的顺序，尽可能和原始的order by顺序一致，这样可以有利于下推
        for (int i = 0; i < implicitOrders.size(); i++) {
            if (i >= leftJoinOrders.size()) {
                return;
            }
            IOrderBy order = implicitOrders.get(i);
            int leftIndex = findOrderByByColumn(leftJoinOrders, order.getColumn());
            int rightIndex = findOrderByByColumn(rightJoinOrders, order.getColumn());

            int index = -1;
            if (leftIndex >= 0 && leftIndex != i) { // 判断位置是否相同
                index = leftIndex;
            } else if (rightIndex >= 0 && rightIndex != i) {
                index = rightIndex;
            }

            if (index >= 0) {
                // 交换位置一下
                IOrderBy tmp = leftJoinOrders.get(i);
                leftJoinOrders.set(i, leftJoinOrders.get(index));
                leftJoinOrders.set(index, tmp);

                tmp = rightJoinOrders.get(i);
                rightJoinOrders.set(i, rightJoinOrders.get(index));
                rightJoinOrders.set(index, tmp);
            } else {
                return;// join列中没有order by的字段，直接退出
            }
        }
    }

    /**
     * 尝试对比父节点中的orderby和子节点的orderby顺序，如果前缀一致，则找出末尾的order by字段进行返回
     * 
     * <pre>
     * 比如 
     * 1. 
     *  父节点：order by c1 ,c2 ,c3
     *  子节点: order by c1, c2
     *  
     * 返回为c3
     * 
     * 2. 
     *  父节点：order by c2 ,c3 
     *  子节点: order by c1, c2，不存在limit
     *  
     * 返回为c2,c3
     * 
     * 3. 
     *  父节点：order by c2 ,c3 
     *  子节点: order by c1, c2，存在limit
     *  
     * 返回为空
     * 
     * 4. 
     *  父节点：order by c1, c2 ,c3
     *  子节点: 无
     *  
     * 返回为c1,c2,c3
     * 
     * 5. 
     *  父节点：order by count(*)  (函数不下推)
     *  子节点: 无
     * 
     * 返回空
     * </pre>
     */
    private static List<IOrderBy> getPushOrderBys(QueryTreeNode qtn, List<IOrderBy> implicitOrderBys,
                                                  QueryTreeNode child, boolean forcePush) {
        List<IOrderBy> newOrderBys = new ArrayList<IOrderBy>();
        List<IOrderBy> targetOrderBys = child.getOrderBys();
        if (implicitOrderBys == null || implicitOrderBys.size() == 0) {
            return new ArrayList<IOrderBy>();
        }
        for (int i = 0; i < implicitOrderBys.size(); i++) {
            IOrderBy order = implicitOrderBys.get(i);
            ISelectable column = qtn.findColumn(order.getColumn());// 找到select或者是meta中的字段
            if (!(column != null && column instanceof IColumn)) {
                // 可能orderby的字段为当前select的函数列
                return new ArrayList<IOrderBy>();
            }

            // 在子节点中找一次，转化为子节点中的字段信息，比如表名，这样才可以和orderby字段做比较
            column = child.findColumn(column);
            if (!(column != null && column instanceof IColumn)) {
                // 可能orderby的字段为当前select的函数列
                return new ArrayList<IOrderBy>();
            }

            forcePush &= (child.getLimitFrom() == null && child.getLimitTo() == null);
            if (!forcePush) {
                // 如果非强制下推，判断一下orderby顺序
                if (targetOrderBys != null && targetOrderBys.size() > i) {
                    IOrderBy targetOrder = targetOrderBys.get(i);
                    if (!(column.equals(targetOrder.getColumn()) && order.getDirection()
                        .equals(targetOrder.getDirection()))) {
                        // 如果不相同
                        return new ArrayList<IOrderBy>();
                    }
                } else {
                    // 如果出现父类的长度>子类
                    IOrderBy newOrder = order.copy().setColumn(column.copy().setAlias(null));
                    newOrderBys.add(newOrder);
                }
            } else {
                // 直接复制
                IOrderBy newOrder = order.copy().setColumn(column.copy().setAlias(null));
                newOrderBys.add(newOrder);
            }
        }

        if (!newOrderBys.isEmpty() && forcePush) {
            // 干掉子节点原本的order by
            child.setOrderBys(new ArrayList<IOrderBy>());
        }
        return newOrderBys;
    }

    /**
     * <pre>
     * 1. 尝试组合group by和join列的排序字段，以join列顺序为准，重排groupBys顺序 
     * 2. 尝试组合order by列和distinct列的排序字段，以order列顺序为准，重排distinct顺序
     * </pre>
     */
    private static List<IOrderBy> getPushOrderBysCombileOrderbyColumns(List<IOrderBy> matchOrders,
                                                                       List<IOrderBy> standardOrders) {
        List<IOrderBy> newOrderbys = new ArrayList<IOrderBy>();
        for (IOrderBy standardOrder : standardOrders) {
            if (findOrderByByColumn(matchOrders, standardOrder.getColumn()) >= 0) {
                newOrderbys.add(standardOrder);
            } else {
                return new ArrayList<IOrderBy>(); // 返回一般的顺序没用
            }
        }

        for (IOrderBy matchOrder : matchOrders) {
            // 找到join column中没有的进行添加
            if (findOrderByByColumn(newOrderbys, matchOrder.getColumn()) < 0) {
                newOrderbys.add(matchOrder);
            }
        }

        return newOrderbys;
    }

    /**
     * 尝试查找一个同名的排序字段，返回下标，-1代表没找到
     */
    private static int findOrderByByColumn(List<IOrderBy> orderbys, ISelectable column) {
        for (int i = 0; i < orderbys.size(); i++) {
            IOrderBy order = orderbys.get(i);
            if (order.getColumn().equals(column)) {
                return i;
            }
        }

        return -1;
    }

    private static void pushJoinOrder(List<IOrderBy> orders, QueryTreeNode qn, boolean isUedForIndexJoinPK) {
        if (orders != null && !orders.isEmpty()) {
            for (IOrderBy order : orders) {
                if (qn.hasColumn(order.getColumn())) {
                    qn.orderBy(order.getColumn(), order.getDirection());
                } else if (isUedForIndexJoinPK) {
                    // 尝试忽略下表名查找一下
                    ISelectable newC = order.getColumn().copy().setTableName(null);
                    if (qn.hasColumn(newC)) {
                        qn.orderBy(order.getColumn(), order.getDirection());
                    }
                }
            }

            qn.build();
        }
    }

    /**
     * 检查下group列是否为分库键列，无顺序要求
     * 
     * @return
     */
    private static boolean isGroupByShardColumns(List<IOrderBy> groupBys, QueryTreeNode qtn) {
        if (!(qtn instanceof TableNode)) {
            return false;
        }

        // 后续可考虑下join merge join，join存在groupBy为单库的分库键
        TableNode node = (TableNode) qtn;
        List<String> shardColumns = OptimizerContext.getContext().getRule().getSharedColumns(node.getTableName());
        List<IOrderBy> newGroupBys = new ArrayList<IOrderBy>();
        for (String shardColumn : shardColumns) {
            boolean isFound = false;
            for (IOrderBy groupBy : groupBys) {
                // 都是大写，直接进行比较
                if (groupBy.getColumn() instanceof IFunction) {
                    return false;
                } else if (groupBy.getColumn().getColumnName().equals(shardColumn)) {
                    // groupBy的列来自于select或者tableMeta，只比较列名，不比较别名
                    isFound = true;
                    newGroupBys.add(groupBy);
                    break;
                }
            }

            if (!isFound) {
                // 找不到分库键
                return false;
            }
        }

        // if (groupBys.size() > newGroupBys.size()) {
        // if (!adjustOrders) {
        // return false;
        // }
        // // 可能是group by name,id. 对应id为分库键，而name为普通字段，也可以下推到单库上执行
        // // 将多出来的非分库键添加到newGroupBys中
        // int size = newGroupBys.size();
        // int i = 0;
        // for (IOrderBy groupBy : groupBys) {
        // int index = findOrderByByColumn(newGroupBys, groupBy.getColumn());
        // if (index == -1) {
        // if (i < size && !adjustOrders) {
        // // 说明groupby前序中有个非分库键的列
        // return false;
        // } else {
        // newGroupBys.add(groupBy);
        // }
        // }
        //
        // i++;
        // }
        //
        // groupBys.clear();
        // groupBys.addAll(newGroupBys);// 将newGroupBys的顺序返回
        // }

        // 所有都找到
        return true;
    }
}
