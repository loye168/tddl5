package com.taobao.tddl.optimizer.costbased.chooser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.monitor.Monitor;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.SchemaManager;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.delegate.NodeDelegate;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.LoadDataNode;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IGroupFilter;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.costbased.FilterPreProcessor;
import com.taobao.tddl.optimizer.costbased.SubQueryPreProcessor;
import com.taobao.tddl.optimizer.costbased.esitimater.Cost;
import com.taobao.tddl.optimizer.costbased.esitimater.CostEsitimaterFactory;
import com.taobao.tddl.optimizer.exception.EmptyResultFilterException;
import com.taobao.tddl.optimizer.exception.OptimizerException;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;
import com.taobao.tddl.rule.model.Field;
import com.taobao.tddl.rule.model.TargetDB;

/**
 * <pre>
 * 1. 根据Rule计算分库分表，并设置执行计划的executeOn()
 * 2. 如果存在多个执行目标库，构造为Merge查询树
 * 
 * </pre>
 * 
 * @author Dreamond
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 * @since 5.0.0
 */
public class DataNodeChooser {

    private static final String UNDECIDED     = "undecided";
    private static Pattern      suffixPattern = Pattern.compile("\\d+$"); // 提取字符串最后的数字

    public static ASTNode shard(ASTNode dne, Parameters parameterSettings, Map<String, Object> extraCmd) {
        try {
            // 针对非batch，绑定变量后，优化语法树
            if (parameterSettings == null || parameterSettings != null && !parameterSettings.isBatch()) {
                dne.assignment(parameterSettings);// null也要做一次，可能存在nextval
                // 绑定变量后，再做一次
                if (dne instanceof DMLNode) {
                    ((DMLNode) dne).setNode((TableNode) FilterPreProcessor.optimize(((DMLNode) dne).getNode(),
                        false,
                        extraCmd));
                } else if (dne instanceof QueryTreeNode) {
                    dne = FilterPreProcessor.optimize(((QueryTreeNode) dne), false, extraCmd);
                }
            }
            if (dne instanceof LoadDataNode) {
                return shardLoad((LoadDataNode) dne, parameterSettings, extraCmd);
            }
            if (dne instanceof DMLNode) {
                if (dne instanceof InsertNode) {
                    return shardInsert((InsertNode) dne, parameterSettings, extraCmd);
                }

                if (dne instanceof UpdateNode) {
                    return shardUpdate((UpdateNode) dne, parameterSettings, extraCmd);
                }
                if (dne instanceof DeleteNode) {
                    return shardDelete((DeleteNode) dne, parameterSettings, extraCmd);
                }
                if (dne instanceof PutNode) {
                    return shardPut((PutNode) dne, parameterSettings, extraCmd);
                }
            } else if (dne instanceof QueryTreeNode) {
                return shardQuery((QueryTreeNode) dne, parameterSettings, extraCmd, true);
            }

        } catch (EmptyResultFilterException e) {
            e.setAstNode(dne);
            throw e;
        }

        return dne;
    }

    private static QueryTreeNode shardQuery(QueryTreeNode qtn, Parameters parameterSettings,
                                            Map<String, Object> extraCmd, boolean traceIn) {
        List<IFunction> funcs = SubQueryPreProcessor.findAllSubqueryOnFilter(qtn, false);
        // filter中存在子查询，提前处理
        for (IFunction func : funcs) {
            QueryTreeNode query = (QueryTreeNode) func.getArgs().get(0);
            // 非correlated subquery理论上在之前都已经被提前计算过，并进行了assignment替换
            query = shardQuery(query, parameterSettings, extraCmd, traceIn);
            func.getArgs().set(0, query);
        }

        if (qtn instanceof QueryNode) {
            QueryNode query = (QueryNode) qtn;
            QueryTreeNode child = query.getChild();
            child = shardQuery(child, parameterSettings, extraCmd, traceIn);
            // 比如select * from (select * from a where xx > xx group b) where xx
            // > xx limit a,b
            // 子查询如果存在聚合函数，不可下推
            // 子查询如果存在from/to，也不可下推.(主要考虑：如果query本身有过滤条件，是在merge查询的基础上过滤，limit之后。如果下推就是limit之前)
            // 父查询如果存在聚合函数，也不可下推
            if (child instanceof MergeNode
                && !(query.isExistAggregate() || child.isExistAggregate() || child.getLimitFrom() != null
                                                                             && child.getLimitTo() != null)) {
                return buildMergeQuery(query, (MergeNode) child, extraCmd);
            } else {
                query.setChild(child);
                query.setBroadcast(child.isBroadcast());// 传递一下
                query.executeOn(child.getDataNode());
                query.setExistAggregate(child.isExistAggregate());
                return query;
            }
        } else if (qtn instanceof TableNode) {
            // 此时经过join处理后，已经全部转化为kv结构的查询了
            KVIndexNode query = (KVIndexNode) qtn;
            if (query.getTableName().equals(SchemaManager.DUAL)) {
                return query;
            }
            // 构造filter
            IFilter f = FilterUtils.and(query.getKeyFilter(), query.getResultFilter());
            f = FilterUtils.and(f, query.getOtherJoinOnFilter());
            f = FilterUtils.and(f, query.getSubqueryFilter());
            List<TargetDB> dataNodeChoosed = shard(query.getIndexName(), f, true, extraCmd);
            return buildMergeTable(query, dataNodeChoosed, extraCmd, traceIn);
        } else if (qtn instanceof JoinNode) {
            // Join节点应该在数据量大的一段进行
            boolean isPartitionOnPartition = false;
            JoinNode join = (JoinNode) qtn;
            QueryTreeNode left = join.getLeftNode();
            QueryTreeNode right = join.getRightNode();

            /*
             * 如果是分库键join分库键，并且规则相同，则优化成join merge join
             */
            if (chooseJoinMergeJoinForce(extraCmd)) {
                isPartitionOnPartition = true;// 强制开启
            } else if (chooseJoinMergeJoinByRule(extraCmd)) {
                isPartitionOnPartition = isJoinOnPartition(join).flag; // 根据规则判断
            }

            // 处理子节点
            left = shardQuery(left, parameterSettings, extraCmd, traceIn);
            right = shardQuery(right, parameterSettings, extraCmd, traceIn);
            if (isPartitionOnPartition || isJoinOnOneGroup(left, right)) {
                // 尝试构建join merge join，可能会构建失败
                // 失败原因 :
                // 1. 人肉强制开启join merge join的选项
                // 2. 涉及index kv的join查询结构，index的数据和原始数据的分区方式可能不一致
                QueryTreeNode joinMergeJoin = buildJoinMergeJoin(join, left, right, extraCmd);
                if (joinMergeJoin != null) {
                    return joinMergeJoin;
                }
            }

            // 不做join merge join
            join.setLeftNode(left);
            // NestedLoop情况下
            // 如果右边是多个表，则分库需要再执行层根据左边的结果做
            if (!(right instanceof MergeNode && (join.getJoinStrategy() == JoinStrategy.INDEX_NEST_LOOP || join.getJoinStrategy() == JoinStrategy.NEST_LOOP_JOIN))) {
                join.setRightNode(right);
            } else {
                if (right.isSubQuery()) {
                    // 子表会采取BLOCK_LOOP_JOIN模式，一次性取完结果
                    join.setRightNode(right);
                } else {
                    right = new MergeNode();
                    // 右边运行时计算
                    ((MergeNode) right).merge(join.getRightNode());
                    ((MergeNode) right).setSharded(false);
                    join.getRightNode().executeOn(UNDECIDED);
                    right.setBroadcast(false);
                    right.build();
                    join.setRightNode(right);
                }
            }

            String dataNode = join.getLeftNode().getDataNode();
            // 对于未决的IndexNestedLoop，join应该在左节点执行
            if (right instanceof MergeNode && !((MergeNode) right).isSharded()) {
                join.executeOn(dataNode);
                right.executeOn(join.getDataNode());
            } else {
                // 选择一个执行代价最小的节点
                Cost leftCost = CostEsitimaterFactory.estimate(left);
                Cost rightCost = CostEsitimaterFactory.estimate(right);
                dataNode = leftCost.getRowCount() > rightCost.getRowCount() ? join.getLeftNode().getDataNode() : join.getRightNode()
                    .getDataNode();
                join.executeOn(dataNode);
            }

            join.setBroadcast(left.isBroadcast() && right.isBroadcast());
            return join;
        } else if (qtn instanceof MergeNode) {
            // 一个query出现or可能会走到index merge，会拆分为merge合并两个请求
            // 为merge选择执行节点
            // 很多方案...
            // 两路归并?
            // 都发到一台机器上？
            // 目前的方案是都发到一台机器上
            // 对Merge的每个子节点的rowCount进行排序
            // 找出rowCount最大的子节点
            // merge应该在该机器上执行，其他机器的数据都发送给它
            MergeNode merge = (MergeNode) qtn;
            List<ASTNode> subNodes = merge.getChildren();
            merge = new MergeNode();
            merge.setUnion(((MergeNode) qtn).isUnion());
            long maxRowCount = 0;
            String maxRowCountDataNode = subNodes.get(0).getDataNode();
            for (int i = 0; i < subNodes.size(); i++) {
                QueryTreeNode child = (QueryTreeNode) subNodes.get(i);
                child = shardQuery(child, parameterSettings, extraCmd, traceIn);
                subNodes.set(i, child);
                Cost childCost = CostEsitimaterFactory.estimate(child);
                if (childCost.getRowCount() > maxRowCount) {
                    maxRowCount = childCost.getRowCount();
                    maxRowCountDataNode = child.getDataNode();
                }
            }

            if (maxRowCountDataNode == null) {
                maxRowCountDataNode = subNodes.get(0).getDataNode();
            }

            merge.executeOn(maxRowCountDataNode);
            merge.merge(subNodes);
            merge.setSharded(true);
            merge.setBroadcast(false);
            merge.build();
            return merge;
        }

        return qtn;
    }

    private static ASTNode shardInsert(InsertNode dne, Parameters parameterSettings, Map<String, Object> extraCmd) {

        String indexName = null;
        if (dne.getNode() instanceof KVIndexNode) {
            indexName = ((KVIndexNode) dne.getNode()).getIndexName();
        } else {
            indexName = dne.getNode().getTableMeta().getPrimaryIndex().getName();
        }

        boolean broadcast = isBroadcast(indexName) && chooseBroadWrite(extraCmd);
        // 处理batch
        if (parameterSettings != null && parameterSettings.isBatch()) {
            if (dne.getSelectNode() != null) {
                throw new OptimizerException("insert select not support in batch");
            }

            Map<List<String>, List<Integer>> indexs = new HashMap<List<String>, List<Integer>>();
            for (int i = 0; i < parameterSettings.getBatchSize(); i++) {
                // 做一下绑定变量
                dne.assignment(parameterSettings.cloneByBatchIndex(i));
                // 根据规则计算
                IFilter insertFilter = createFilter(dne.getColumns(), dne.getValues());
                List<TargetDB> dataNodeChoosed = shard(indexName, insertFilter, true, extraCmd);
                TargetDB itarget = dataNodeChoosed.get(0);
                if (dataNodeChoosed.size() == 1 && itarget.getTableNameMap() != null
                    && itarget.getTableNameMap().size() == 1) {
                    // 构造key
                    List<String> dbAndTab = Arrays.asList(itarget.getDbIndex(), itarget.getTableNameMap()
                        .keySet()
                        .iterator()
                        .next());
                    List<Integer> ids = indexs.get(dbAndTab);
                    if (ids == null) {
                        ids = new ArrayList<Integer>();
                        indexs.put(dbAndTab, ids);
                    }
                    ids.add(i);
                } else {
                    throw new OptimizerException("insert not support muti tables, parameter is " + parameterSettings);
                }
            }

            List<InsertNode> nodes = new ArrayList<InsertNode>();
            for (Map.Entry<List<String>, List<Integer>> entry : indexs.entrySet()) {
                InsertNode node = buildOneInsertNode(dne, indexName, entry.getKey().get(0), entry.getKey().get(1));
                node.setBatchIndexs(entry.getValue());
                nodes.add(node);
            }

            if (nodes.size() > 1) {
                // 构造merge
                MergeNode merge = new MergeNode();
                for (ASTNode node : nodes) {
                    merge.merge(node);
                }

                merge.executeOn(nodes.get(0).getDataNode()).setExtra(nodes.get(0).getExtra());
                merge.build();
                return merge;
            } else if (broadcast) {
                // 处理广播表
                return buildMergeBroadcast(nodes.get(0));
            } else {
                return nodes.get(0);
            }
        } else if (dne.isMultiValues()) {// 处理下insert多value
            if (dne.getSelectNode() != null) {
                throw new OptimizerException("insert select not support in multi values sql");
            }

            int multiSize = dne.getMultiValuesSize();
            Map<List<String>, List<List<Object>>> multiValues = new HashMap<List<String>, List<List<Object>>>();
            for (int i = 0; i < multiSize; i++) {
                // 根据规则计算
                List<Object> values = dne.getValues(i);
                IFilter insertFilter = createFilter(dne.getColumns(), dne.getValues(i));
                List<TargetDB> dataNodeChoosed = shard(indexName, insertFilter, true, extraCmd);
                TargetDB itarget = dataNodeChoosed.get(0);
                if (dataNodeChoosed.size() == 1 && itarget.getTableNameMap() != null
                    && itarget.getTableNameMap().size() == 1) {
                    // 构造key
                    List<String> dbAndTab = Arrays.asList(itarget.getDbIndex(), itarget.getTableNameMap()
                        .keySet()
                        .iterator()
                        .next());
                    List<List<Object>> vv = multiValues.get(dbAndTab);
                    if (vv == null) {
                        vv = new ArrayList<List<Object>>();
                        multiValues.put(dbAndTab, vv);
                    }

                    vv.add(values);
                } else {
                    throw new OptimizerException("insert must contain all shard columns");
                }
            }

            if (multiValues.size() > 1) {
                MergeNode merge = new MergeNode();
                for (Map.Entry<List<String>, List<List<Object>>> entry : multiValues.entrySet()) {
                    List<String> names = entry.getKey();
                    InsertNode node = buildOneInsertNode(dne, indexName, names.get(0), names.get(1));
                    node.setMultiValues(entry.getValue()); // 修改一下对应的multiValues
                    merge.merge(node);
                }

                merge.executeOn(merge.getChild().getDataNode()).setExtra(merge.getChild().getExtra());
                merge.build();
                return merge;
            } else {
                List<String> names = multiValues.keySet().iterator().next();
                InsertNode node = buildOneInsertNode(dne, indexName, names.get(0), names.get(1));
                if (broadcast) {
                    // 处理广播表
                    return buildMergeBroadcast(node);
                } else {
                    return node;
                }
            }
        } else {
            if (dne.getSelectNode() == null) {
                // 根据规则计算
                IFilter insertFilter = createFilter(dne.getColumns(), dne.getValues());
                List<TargetDB> dataNodeChoosed = shard(indexName, insertFilter, true, extraCmd);
                TargetDB itarget = dataNodeChoosed.get(0);
                dne.executeOn(itarget.getDbIndex());
                if (dataNodeChoosed.size() == 1 && itarget.getTableNameMap() != null
                    && itarget.getTableNameMap().size() == 1) {
                    InsertNode result = buildOneInsertNode(dne,
                        indexName,
                        itarget.getDbIndex(),
                        itarget.getTableNameMap().keySet().iterator().next());
                    if (broadcast) {
                        // 处理广播表
                        return buildMergeBroadcast(result);
                    } else {
                        return result;
                    }
                } else {
                    throw new OptimizerException("insert not support muti tables");
                }
            } else {
                QueryTreeNode insert = shardQuery(dne.getNode(), parameterSettings, extraCmd, true);
                QueryTreeNode select = shardQuery(dne.getSelectNode(), parameterSettings, extraCmd, true);
                // insert select mode, 需要判断分库分表是否完全一致
                PartitionJoinResult result = isInsertSelectOnPartition(dne.getNode(), dne.getSelectNode());
                if (result.flag || isJoinOnOneGroup(insert, select)) {
                    // 如果join group相同，则继续判断
                    ASTNode merge = buildMergeInsertSelect(dne, insert, select, extraCmd);
                    if (merge != null) {
                        if (broadcast && select.isBroadcast()) { // 都是广播表
                            // 如果是广播表，一定是单表
                            dne.setNode((TableNode) insert);
                            dne.setSelectNode(select);
                            dne.executeOn(insert.getDataNode());
                            return buildMergeBroadcast(dne);
                        } else {
                            return merge;
                        }
                    }
                }

                // 如果满足单库单表的逻辑(不能包含merge节点，可以包含query节点)，并且大家都在一个库上
                String insertGroup = getOneGroup(insert);
                String selectGroup = getOneGroup(select);
                if (insertGroup != null && selectGroup != null && StringUtils.equals(insertGroup, selectGroup)) {
                    dne.setNode((TableNode) insert);
                    dne.setSelectNode(select);
                    dne.executeOn(insert.getDataNode());
                    return dne;
                }

                throw new OptimizerException("insert select not support cross db");
                // dne.setSelectNode(select);
                // dne.getNode().executeOn(UNDECIDED); // 设置为未决节点,运行时动态算
                // return dne;
            }
        }
    }

    private static ASTNode shardLoad(LoadDataNode dne, Parameters parameterSettings, Map<String, Object> extraCmd) {
        dne = dne.copy();
        String tableName = dne.getTableName();
        List<TargetDB> targets = shard(tableName, null, false, extraCmd);
        TargetDB target = targets.get(0);
        if (targets.size() == 1 && target.getTableNameMap() != null && target.getTableNameMap().size() == 1) {
            dne.executeOn(target.getDbIndex());
        } else {
            throw new OptimizerException("load data support single group and single table only, sql:" + dne.getSql());
        }

        return dne;
    }

    private static ASTNode shardUpdate(UpdateNode dne, Parameters parameterSettings, Map<String, Object> extraCmd) {

        // 处理batch
        // case1 :
        // update xxx where id > ? and id < ?
        // 针对绑定变量：
        // a. id > 1 and id < 4 , 会得到 2, 3
        // b. id > 2 and id < 5 , 会得到 3, 4
        // 会得到3个表的batch. 2(a) 3(a,b) 4(b)
        //
        // case 2 :
        // update xxx where id in (?,?)
        // 针对绑定变量：
        // a. id in (2, 3) , 会得到 2, 3
        // b. id in (3, 4) , 会得到 3, 4
        // 会得到3个表的batch. 2(a) 3(a,b) 4(b)
        // 注意:发送给表的sql均为 id in (?,?) ，这里不会是经过in优化的sql
        boolean broadcast = isBroadcast(dne.getNode().getTableName()) && chooseBroadWrite(extraCmd);
        if (parameterSettings != null && parameterSettings.isBatch()) {
            Map<List<String>, List<Integer>> indexs = new HashMap<List<String>, List<Integer>>();
            QueryTreeNode whereNode = null;
            for (int i = 0; i < parameterSettings.getBatchSize(); i++) {
                // 做一下绑定变量
                dne.assignment(parameterSettings.cloneByBatchIndex(i));
                // 根据规则计算,不处理in优化
                QueryTreeNode qtn = shardQuery(dne.getNode(), parameterSettings, extraCmd, false);
                List<ASTNode> subs = new ArrayList();
                if (qtn instanceof MergeNode) {
                    subs.addAll(qtn.getChildren());
                } else {
                    subs.add(qtn);
                }

                // 一定会有个where node
                whereNode = (QueryTreeNode) subs.get(0);
                for (ASTNode sub : subs) {
                    if (!(sub instanceof TableNode)) {
                        throw new NotSupportException("update中暂不支持按照索引进行查询");
                    }

                    // 构造key
                    List<String> dbAndTab = Arrays.asList(sub.getDataNode(), ((TableNode) sub).getActualTableName());
                    List<Integer> ids = indexs.get(dbAndTab);
                    if (ids == null) {
                        ids = new ArrayList<Integer>();
                        indexs.put(dbAndTab, ids);
                    }
                    ids.add(i);
                }

            }

            List<UpdateNode> nodes = new ArrayList<UpdateNode>();
            for (Map.Entry<List<String>, List<Integer>> entry : indexs.entrySet()) {
                UpdateNode node = buildOneQueryUpdate(whereNode.copy(),
                    dne,
                    entry.getKey().get(0),
                    entry.getKey().get(1));
                node.setBatchIndexs(entry.getValue());
                nodes.add(node);
            }

            if (nodes.size() > 1) {
                // 构造merge
                MergeNode merge = new MergeNode();
                for (ASTNode node : nodes) {
                    merge.merge(node);
                }

                merge.executeOn(nodes.get(0).getDataNode()).setExtra(nodes.get(0).getExtra());
                merge.build();
                return merge;
            } else if (broadcast) {
                return buildMergeBroadcast(nodes.get(0));
            } else {
                return nodes.get(0);
            }
        } else {
            QueryTreeNode qtn = shardQuery(dne.getNode(), parameterSettings, extraCmd, true);
            List<ASTNode> subs = new ArrayList();
            if (qtn instanceof MergeNode) {
                subs.addAll(qtn.getChildren());
            } else {
                subs.add(qtn);
            }

            if (subs.size() > 1) {
                MergeNode updateMerge = new MergeNode();
                for (ASTNode sub : subs) {
                    updateMerge.merge(buildOneQueryUpdate((QueryTreeNode) sub, dne));
                }
                updateMerge.executeOn(updateMerge.getChild().getDataNode());
                updateMerge.build();
                return updateMerge;
            } else {
                UpdateNode node = buildOneQueryUpdate((QueryTreeNode) subs.get(0), dne);
                if (broadcast) {
                    // 处理广播表
                    return buildMergeBroadcast(node);
                } else {
                    return node;
                }
            }

        }
    }

    private static ASTNode shardDelete(DeleteNode dne, Parameters parameterSettings, Map<String, Object> extraCmd) {

        // 处理batch
        // case1 :
        // update xxx where id > ? and id < ?
        // 针对绑定变量：
        // a. id > 1 and id < 4 , 会得到 2, 3
        // b. id > 2 and id < 5 , 会得到 3, 4
        // 会得到3个表的batch. 2(a) 3(a,b) 4(b)
        //
        // case 2 :
        // update xxx where id in (?,?)
        // 针对绑定变量：
        // a. id in (2, 3) , 会得到 2, 3
        // b. id in (3, 4) , 会得到 3, 4
        // 会得到3个表的batch. 2(a) 3(a,b) 4(b)
        // 注意:发送给表的sql均为 id in (?,?) ，这里不会是经过in优化的sql
        boolean broadcast = isBroadcast(dne.getNode().getTableName()) && chooseBroadWrite(extraCmd);
        if (parameterSettings != null && parameterSettings.isBatch()) {
            Map<List<String>, List<Integer>> indexs = new HashMap<List<String>, List<Integer>>();
            QueryTreeNode whereNode = null;
            for (int i = 0; i < parameterSettings.getBatchSize(); i++) {
                // 做一下绑定变量
                dne.assignment(parameterSettings.cloneByBatchIndex(i));
                // 根据规则计算, batch不处理in优化
                QueryTreeNode qtn = shardQuery(dne.getNode(), parameterSettings, extraCmd, false);
                List<ASTNode> subs = new ArrayList();
                if (qtn instanceof MergeNode) {
                    subs.addAll(qtn.getChildren());
                } else {
                    subs.add(qtn);
                }

                // 一定会有个where node
                whereNode = (QueryTreeNode) subs.get(0);
                for (ASTNode sub : subs) {
                    if (!(sub instanceof TableNode)) {
                        throw new NotSupportException("update中暂不支持按照索引进行查询");
                    }

                    // 构造key
                    List<String> dbAndTab = Arrays.asList(sub.getDataNode(), ((TableNode) sub).getActualTableName());
                    List<Integer> ids = indexs.get(dbAndTab);
                    if (ids == null) {
                        ids = new ArrayList<Integer>();
                        indexs.put(dbAndTab, ids);
                    }
                    ids.add(i);
                }
            }

            List<DeleteNode> nodes = new ArrayList<DeleteNode>();
            for (Map.Entry<List<String>, List<Integer>> entry : indexs.entrySet()) {
                DeleteNode node = buildOneQueryDelete(whereNode.copy(),
                    dne,
                    entry.getKey().get(0),
                    entry.getKey().get(1));
                node.setBatchIndexs(entry.getValue());
                nodes.add(node);
            }

            if (nodes.size() > 1) {
                // 构造merge
                MergeNode merge = new MergeNode();
                for (ASTNode node : nodes) {
                    merge.merge(node);
                }

                merge.executeOn(nodes.get(0).getDataNode()).setExtra(nodes.get(0).getExtra());
                merge.build();
                return merge;
            } else if (broadcast) {
                return buildMergeBroadcast(nodes.get(0));
            } else {
                return nodes.get(0);
            }
        } else {
            QueryTreeNode qtn = shardQuery(dne.getNode(), parameterSettings, extraCmd, true);
            List<ASTNode> subs = new ArrayList();
            if (qtn instanceof MergeNode) {
                subs.addAll(qtn.getChildren());
            } else {
                subs.add(qtn);
            }

            if (subs.size() > 1) {
                MergeNode deleteMerge = new MergeNode();
                for (ASTNode sub : subs) {
                    deleteMerge.merge(buildOneQueryDelete((QueryTreeNode) sub, dne));
                }
                deleteMerge.executeOn(deleteMerge.getChild().getDataNode());
                deleteMerge.build();
                return deleteMerge;
            } else {
                DeleteNode node = buildOneQueryDelete((QueryTreeNode) subs.get(0), dne);
                if (broadcast) {
                    // 处理广播表
                    return buildMergeBroadcast(node);
                } else {
                    return node;
                }
            }
        }
    }

    private static ASTNode shardPut(PutNode dne, Parameters parameterSettings, Map<String, Object> extraCmd) {
        String indexName = null;
        if (dne.getNode() instanceof KVIndexNode) {
            indexName = ((KVIndexNode) dne.getNode()).getIndexName();
        } else {
            indexName = dne.getNode().getTableMeta().getPrimaryIndex().getName();
        }

        boolean broadcast = isBroadcast(indexName) && chooseBroadWrite(extraCmd);
        // 处理batch
        if (parameterSettings != null && parameterSettings.isBatch()) {
            if (dne.getSelectNode() != null) {
                throw new OptimizerException("replace insert select not support in batch");
            }
            Map<List<String>, List<Integer>> indexs = new HashMap<List<String>, List<Integer>>();
            for (int i = 0; i < parameterSettings.getBatchSize(); i++) {
                // 做一下绑定变量
                dne.assignment(parameterSettings.cloneByBatchIndex(i));
                // 根据规则计算
                IFilter insertFilter = createFilter(dne.getColumns(), dne.getValues());
                List<TargetDB> dataNodeChoosed = shard(indexName, insertFilter, true, extraCmd);
                TargetDB itarget = dataNodeChoosed.get(0);
                if (dataNodeChoosed.size() == 1 && itarget.getTableNameMap() != null
                    && itarget.getTableNameMap().size() == 1) {
                    // 构造key
                    List<String> dbAndTab = Arrays.asList(itarget.getDbIndex(), itarget.getTableNameMap()
                        .keySet()
                        .iterator()
                        .next());
                    List<Integer> ids = indexs.get(dbAndTab);
                    if (ids == null) {
                        ids = new ArrayList<Integer>();
                        indexs.put(dbAndTab, ids);
                    }
                    ids.add(i);
                } else {
                    throw new OptimizerException("replace must contain all shard columns");
                }

            }

            List<PutNode> nodes = new ArrayList<PutNode>();
            for (Map.Entry<List<String>, List<Integer>> entry : indexs.entrySet()) {
                PutNode node = buildOnePutNode(dne, indexName, entry.getKey().get(0), entry.getKey().get(1));
                node.setBatchIndexs(entry.getValue());
                nodes.add(node);
            }

            if (nodes.size() > 1) {
                // 构造merge
                MergeNode merge = new MergeNode();
                for (ASTNode node : nodes) {
                    merge.merge(node);
                }

                merge.executeOn(nodes.get(0).getDataNode()).setExtra(nodes.get(0).getExtra());
                merge.build();
                return merge;
            } else if (broadcast) {
                // 处理广播表
                return buildMergeBroadcast(nodes.get(0));
            } else {
                return nodes.get(0);
            }
        } else if (dne.isMultiValues()) { // 处理下insert多value
            if (dne.getSelectNode() != null) {
                throw new OptimizerException("replace insert select not support in batch");
            }
            int multiSize = dne.getMultiValuesSize();
            Map<List<String>, List<List<Object>>> multiValues = new HashMap<List<String>, List<List<Object>>>();
            for (int i = 0; i < multiSize; i++) {
                // 根据规则计算
                List<Object> values = dne.getValues(i);
                IFilter insertFilter = createFilter(dne.getColumns(), dne.getValues(i));
                List<TargetDB> dataNodeChoosed = shard(indexName, insertFilter, true, extraCmd);
                TargetDB itarget = dataNodeChoosed.get(0);
                if (dataNodeChoosed.size() == 1 && itarget.getTableNameMap() != null
                    && itarget.getTableNameMap().size() == 1) {
                    // 构造key
                    List<String> dbAndTab = Arrays.asList(itarget.getDbIndex(), itarget.getTableNameMap()
                        .keySet()
                        .iterator()
                        .next());
                    List<List<Object>> vv = multiValues.get(dbAndTab);
                    if (vv == null) {
                        vv = new ArrayList<List<Object>>();
                        multiValues.put(dbAndTab, vv);
                    }

                    vv.add(values);
                } else {
                    throw new OptimizerException("put not support muti tables");
                }
            }

            if (multiValues.size() > 1) {
                // 构造merge
                MergeNode merge = new MergeNode();
                for (Map.Entry<List<String>, List<List<Object>>> entry : multiValues.entrySet()) {
                    List<String> names = entry.getKey();
                    PutNode node = buildOnePutNode(dne, indexName, names.get(0), names.get(1));
                    node.setMultiValues(entry.getValue()); // 修改一下对应的multiValues
                    merge.merge(node);
                }

                merge.executeOn(merge.getChild().getDataNode()).setExtra(merge.getChild().getExtra());
                merge.build();
                return merge;
            } else {
                List<String> names = multiValues.keySet().iterator().next();
                PutNode node = buildOnePutNode(dne, indexName, names.get(0), names.get(1));
                if (broadcast) {
                    // 处理广播表
                    return buildMergeBroadcast(node);
                } else {
                    return node;
                }
            }
        } else {
            if (dne.getSelectNode() == null) {
                // 根据规则计算
                IFilter insertFilter = createFilter(dne.getColumns(), dne.getValues());
                List<TargetDB> dataNodeChoosed = shard(indexName, insertFilter, true, extraCmd);
                TargetDB itarget = dataNodeChoosed.get(0);
                dne.executeOn(itarget.getDbIndex());
                if (dataNodeChoosed.size() == 1 && itarget.getTableNameMap() != null
                    && itarget.getTableNameMap().size() == 1) {
                    PutNode node = buildOnePutNode(dne, indexName, itarget.getDbIndex(), itarget.getTableNameMap()
                        .keySet()
                        .iterator()
                        .next());
                    if (broadcast) {
                        // 处理广播表
                        return buildMergeBroadcast(node);
                    } else {
                        return node;
                    }
                } else {
                    throw new OptimizerException("put not support muti tables");
                }
            } else {
                QueryTreeNode insert = shardQuery(dne.getNode(), parameterSettings, extraCmd, true);
                QueryTreeNode select = shardQuery(dne.getSelectNode(), parameterSettings, extraCmd, true);
                // insert select mode, 需要判断分库分表是否完全一致
                PartitionJoinResult result = isInsertSelectOnPartition(dne.getNode(), dne.getSelectNode());
                if (result.flag || isJoinOnOneGroup(insert, select)) {
                    // 如果join group相同，则继续判断
                    ASTNode merge = buildMergeInsertSelect(dne, insert, select, extraCmd);
                    if (broadcast && select.isBroadcast()) { // 都是广播表
                        // 如果是广播表，一定是单表
                        dne.setNode((TableNode) insert);
                        dne.setSelectNode(select);
                        dne.executeOn(insert.getDataNode());
                        return buildMergeBroadcast(dne);
                    } else {
                        return merge;
                    }
                }

                // 如果满足单库单表的逻辑(不能包含merge节点，可以包含query节点)，并且大家都在一个库上
                String insertGroup = getOneGroup(insert);
                String selectGroup = getOneGroup(select);
                if (insertGroup != null && selectGroup != null && StringUtils.equals(insertGroup, selectGroup)) {
                    dne.setNode((TableNode) insert);
                    dne.setSelectNode(select);
                    dne.executeOn(insert.getDataNode());
                    return dne;
                }

                throw new OptimizerException("insert select not support cross db");
                // dne.setSelectNode(select);
                // dne.getNode().executeOn(UNDECIDED); // 设置为未决节点,运行时动态算
                // return dne;
            }
        }
    }

    // =============== helper method =================

    /**
     * 根据逻辑表名和执行条件计算出执行节点
     */
    private static List<TargetDB> shard(String logicalName, IFilter f, boolean isWrite, Map<String, Object> extraCmd) {
        // boolean isTraceSource = true;
        // if (f != null && f instanceof ILogicalFilter) {
        // if (f.getOperation().equals(OPERATION.OR)) {
        // f = null;
        // isTraceSource = false;
        // }
        // }

        // if (logger.isDebugEnabled()) {
        // logger.warn("shard debug:\n");
        // logger.warn("logicalName:" + logicalName);
        // logger.warn("filter:" + f);
        // logger.warn("isTraceSource:" + isTraceSource);
        // logger.warn("isWrite:" + isWrite);
        // }

        boolean forceAllowFullTableScan = GeneralUtil.getExtraCmdBoolean(extraCmd,
            ConnectionProperties.ALLOW_FULL_TABLE_SCAN,
            false);

        long time = System.currentTimeMillis();
        String tableName = logicalName.split("\\.")[0];
        OptimizerContext.getContext().getSchemaManager().getTable(tableName); // 验证下表是否存在
        List<TargetDB> dataNodeChoosed = OptimizerContext.getContext()
            .getRule()
            .shard(logicalName, f, isWrite, forceAllowFullTableScan);
        if (dataNodeChoosed == null || dataNodeChoosed.isEmpty()) {
            throw new EmptyResultFilterException();
        }

        time = Monitor.monitorAndRenewTime(Monitor.KEY1,
            Monitor.KEY2_TDDL_PARSE,
            Monitor.Key3Success,
            System.currentTimeMillis() - time);
        return dataNodeChoosed;
    }

    private static boolean isBroadcast(String logicalName) {
        String tableName = logicalName.split("\\.")[0];
        return OptimizerContext.getContext().getRule().isBroadCast(tableName);
    }

    private static boolean chooseBroadWrite(Map<String, Object> extraCmd) {
        return GeneralUtil.getExtraCmdBoolean(extraCmd, ConnectionProperties.CHOOSE_BROADCAST_WRITE, true);
    }

    private static class OneDbNodeWithCount {

        List<QueryTreeNode> subs          = new ArrayList();
        long                totalRowCount = 0;
    }

    /**
     * 根据执行的目标节点，构建MergeNode
     */
    private static QueryTreeNode buildMergeTable(TableNode table, List<TargetDB> dataNodeChoosed,
                                                 Map<String, Object> extraCmd, boolean traceIn) {
        long maxRowCount = 0;
        String maxRowCountDataNode = dataNodeChoosed.get(0).getDbIndex();
        List<List<QueryTreeNode>> subs = new ArrayList();
        // 单库单表是大多数场景，此时无需复制执行计划
        // 大部分情况只有一张表
        boolean needCopy = false;
        if (dataNodeChoosed.size() > 0 && dataNodeChoosed.get(0).getTableNameMap().size() > 0) {
            Map<String, Field> tabMap = dataNodeChoosed.get(0).getTableNameMap();
            Entry<String, Field> entry = tabMap.entrySet().iterator().next();
            // 如果存在in，则必须使用复制
            if (traceIn && entry.getValue() != null) {
                needCopy |= traceSourceInFilter(table.getKeyFilter(), entry.getValue().getSourceKeys(), true);
                needCopy |= traceSourceInFilter(table.getResultFilter(), entry.getValue().getSourceKeys(), true);
            }

            if (!chooseShareNode(extraCmd) && tabMap.size() > 1) {
                // 如果不使用share模式，并且多于一张表，那就采用复制模式
                needCopy = true;
            }

            if (!needCopy && tabMap.size() > 1) {
                table = table.copy(); // 针对share模式，需要复制一份
            }

        }

        int index = 0;
        for (TargetDB target : dataNodeChoosed) {
            OneDbNodeWithCount oneDbNodeWithCount = buildMergeTableInOneDB(table, target, index, needCopy, traceIn);
            if (!oneDbNodeWithCount.subs.isEmpty()) {
                subs.add(oneDbNodeWithCount.subs);
            }

            index += target.getTableNameMap().size();
            if (oneDbNodeWithCount.totalRowCount > maxRowCount) {
                maxRowCount = oneDbNodeWithCount.totalRowCount;
                maxRowCountDataNode = target.getDbIndex();
            }

        }

        if (subs.isEmpty()) {
            throw new EmptyResultFilterException();
        } else if (subs.size() == 1 && subs.get(0).size() == 1) {
            return subs.get(0).get(0); // 只有单库
        } else {
            // 多库执行
            MergeNode merge = new MergeNode();
            merge.setAlias(table.getAlias());
            merge.setSubQuery(table.isSubQuery());
            merge.setSubAlias(table.getSubAlias());
            merge.executeOn(maxRowCountDataNode);
            merge.setSubqueryOnFilterId(table.getSubqueryOnFilterId());
            for (List<QueryTreeNode> subList : subs) {
                for (QueryTreeNode sub : subList) {
                    merge.merge(sub);
                }
            }
            merge.setBroadcast(false);// merge不可能是广播表
            merge.build();// build过程中会复制子节点的信息
            return merge;
        }
    }

    /**
     * 构建单库的执行节点
     */
    private static OneDbNodeWithCount buildMergeTableInOneDB(TableNode table, TargetDB targetDB, int baseIndex,
                                                             boolean needCopy, boolean traceIn) {
        long totalRowCount = 0;
        OneDbNodeWithCount oneDbNodeWithCount = new OneDbNodeWithCount();
        Map<String, Field> tabMap = targetDB.getTableNameMap();
        int i = baseIndex;
        for (String targetTable : tabMap.keySet()) {
            TableNode node = table;
            if (needCopy) {
                node = table.copy();

                // tddl的traceSource在分库不分表，和全表扫描时无法使用 mengshi
                if (traceIn && tabMap.get(targetTable) != null && tabMap.get(targetTable).getSourceKeys() != null) {
                    traceSourceInFilter(node.getKeyFilter(), tabMap.get(targetTable).getSourceKeys(), false);
                    traceSourceInFilter(node.getResultFilter(), tabMap.get(targetTable).getSourceKeys(), false);
                }
            }

            if (needCopy) {
                node.setActualTableName(targetTable);
                node.executeOn(targetDB.getDbIndex());
                node.setExtra(getIdentifierExtra((KVIndexNode) node, 0));// 设置标志
                oneDbNodeWithCount.subs.add(node);
            } else {
                node.setActualTableName(targetTable, i);
                node.executeOn(targetDB.getDbIndex(), i);
                node.setExtra(getIdentifierExtra((KVIndexNode) node, i), i);// 设置标志
                // 添加一个代理对象
                oneDbNodeWithCount.subs.add((QueryTreeNode) new NodeDelegate(node, i).getProxy());
                i = i + 1;
            }

            // 暂时先用逻辑表名，以后可能是索引名
            String indexName = null;
            if (node instanceof KVIndexNode) {
                indexName = ((KVIndexNode) node).getKvIndexName();
            } else {
                indexName = node.getTableMeta().getPrimaryIndex().getName();
            }
            node.setBroadcast(OptimizerContext.getContext().getRule().isBroadCast(indexName));
            totalRowCount += CostEsitimaterFactory.estimate(node).getRowCount();
        }

        oneDbNodeWithCount.totalRowCount = totalRowCount;
        return oneDbNodeWithCount;
    }

    /**
     * 更新in操作的values
     */
    private static boolean traceSourceInFilter(IFilter filter, Map<String, Set<Object>> sourceKeys, boolean dryRun) {
        if (sourceKeys == null || filter == null) {
            return false;
        }

        if (filter instanceof IBooleanFilter) {
            if (filter.getOperation().equals(OPERATION.IN)) {
                ISelectable s = (ISelectable) ((IBooleanFilter) filter).getColumn();
                Set<Object> sourceKey = sourceKeys.get(s.getColumnName());
                if (sourceKey != null && !sourceKey.isEmpty()) {
                    if (!dryRun) {
                        // 可能只是验证下是否需要做in处理
                        ((IBooleanFilter) filter).setValues(new ArrayList(sourceKey));
                    }
                    return true;
                }
            }
        } else if (filter instanceof IGroupFilter) {
            boolean existIn = false;
            for (IFilter subFilter : ((IGroupFilter) filter).getSubFilter()) {
                if (subFilter.getOperation().equals(OPERATION.IN)) {
                    ISelectable s = (ISelectable) ((IBooleanFilter) subFilter).getColumn();
                    Set<Object> sourceKey = sourceKeys.get(s.getColumnName());
                    if (sourceKey != null && !sourceKey.isEmpty()) {// 走了规则,在in中有sourceTrace
                        if (!dryRun) {
                            // 可能只是验证下是否需要做in处理
                            ((IBooleanFilter) subFilter).setValues(new ArrayList(sourceKey));
                        }
                        existIn = true;
                    }
                }
            }
            return existIn;
        } else if (filter instanceof ILogicalFilter) {
            boolean existIn = false;
            for (IFilter subFilter : ((ILogicalFilter) filter).getSubFilter()) {
                if (subFilter.getOperation().equals(OPERATION.IN)) {
                    ISelectable s = (ISelectable) ((IBooleanFilter) subFilter).getColumn();
                    Set<Object> sourceKey = sourceKeys.get(s.getColumnName());
                    if (sourceKey != null && !sourceKey.isEmpty()) {// 走了规则,在in中有sourceTrace
                        if (!dryRun) {
                            // 可能只是验证下是否需要做in处理
                            ((IBooleanFilter) subFilter).setValues(new ArrayList(sourceKey));
                        }
                        existIn = true;
                    }
                }
            }
            return existIn;
        }

        return false;
    }

    private static InsertNode buildOneInsertNode(InsertNode dne, String indexName, String dbIndex, String tableName) {
        KVIndexNode query = new KVIndexNode(indexName);
        // 设置为物理的表
        query.executeOn(dbIndex);
        query.setActualTableName(tableName);

        InsertNode insert = dne.copySelf();
        insert.setNode(query);
        insert.executeOn(query.getDataNode());
        insert.build();
        return insert;
    }

    private static PutNode buildOnePutNode(PutNode dne, String indexName, String dbIndex, String tableName) {
        KVIndexNode query = new KVIndexNode(indexName);
        // 设置为物理的表
        query.executeOn(dbIndex);
        query.setActualTableName(tableName);

        PutNode put = dne.copySelf();
        put.setNode(query);
        put.executeOn(query.getDataNode());
        put.build();
        return put;
    }

    private static UpdateNode buildOneQueryUpdate(QueryTreeNode sub, UpdateNode dne, String dbIndex, String tableName) {
        if (sub instanceof TableNode) {
            // 不能使用QueryTreeNode.update()生成node
            // 因为使用了代理模式后，update中会将this做为构造参数，而this并不是生成的代理类对象
            ((TableNode) sub).executeOn(dbIndex);
            ((TableNode) sub).setActualTableName(tableName);
            return buildOneQueryUpdate(sub, dne);
        } else {
            throw new NotSupportException("update中暂不支持按照索引进行查询");
        }
    }

    private static UpdateNode buildOneQueryUpdate(QueryTreeNode sub, UpdateNode dne) {
        UpdateNode update = dne.copySelf();
        update.setNode((TableNode) sub);
        update.executeOn(sub.getDataNode());
        return update;
    }

    private static DeleteNode buildOneQueryDelete(QueryTreeNode sub, DeleteNode dne, String dbIndex, String tableName) {
        if (sub instanceof TableNode) {
            // 不能使用QueryTreeNode.update()生成node
            // 因为使用了代理模式后，update中会将this做为构造参数，而this并不是生成的代理类对象
            ((TableNode) sub).executeOn(dbIndex);
            ((TableNode) sub).setActualTableName(tableName);

            return buildOneQueryDelete(sub, dne);
        } else {
            throw new NotSupportException("delete中暂不支持按照索引进行查询");
        }
    }

    private static DeleteNode buildOneQueryDelete(QueryTreeNode sub, DeleteNode dne) {
        DeleteNode delete = dne.copySelf();
        delete.setNode((TableNode) sub);
        delete.executeOn(sub.getDataNode());
        return delete;
    }

    /**
     * @param node
     * @param groups
     * @return
     */
    private static MergeNode buildMergeBroadcast(DMLNode node) {
        Matrix matrix = OptimizerContext.getContext().getMatrix();
        List<String> groups = new ArrayList<String>();
        for (Group group : matrix.getGroups()) {
            if (!group.getType().isDemo()) {
                groups.add(group.getName());
            }
        }

        String defaultGroup = node.getDataNode();
        groups.remove(defaultGroup);

        MergeNode merge = new MergeNode();
        merge.setDmlByBroadcast(true);
        merge.merge(node);
        for (String group : groups) {
            DMLNode dne = (DMLNode) node.deepCopy();
            setExecuteOn(dne, group);
            dne.executeOn(group);
            merge.merge(dne);
        }
        merge.executeOn(defaultGroup);
        return merge;
    }

    private static QueryTreeNode buildMergeQuery(QueryNode query, MergeNode mergeNode, Map<String, Object> extraCmd) {
        List<QueryNode> mergeQuerys = new LinkedList<QueryNode>();

        for (ASTNode child : mergeNode.getChildren()) {
            // 在未做shard之前就有存在mergeNode的可能，要识别出来
            // 比如OR条件，可能会被拆分为两个Query的Merge，这个经过build之后，会是Merge套Merge或者是Merge套Query
            if (!(child instanceof MergeNode) && child.getExtra() != null) {
                QueryNode newQuery = query.copySelf();// 只复制自己，不复制子节点
                newQuery.setChild((QueryTreeNode) child);
                newQuery.executeOn(child.getDataNode());
                newQuery.setExtra(child.getExtra());
                newQuery.setBroadcast(child.isBroadcast());
                mergeQuerys.add(newQuery);
            }
        }

        if (mergeQuerys.size() > 1) {
            MergeNode merge = new MergeNode();
            merge.setAlias(query.getAlias());
            merge.setSubQuery(query.isSubQuery());
            merge.setSubAlias(query.getSubAlias());
            // 复制子节点的limit/to信息
            merge.setLimitFrom(query.getLimitFrom());
            merge.setLimitTo(query.getLimitTo());
            merge.setSubqueryOnFilterId(query.getSubqueryOnFilterId());
            for (QueryNode q : mergeQuerys) {
                merge.merge(q);
            }

            merge.executeOn(mergeQuerys.get(0).getDataNode()).setExtra(mergeQuerys.get(0).getExtra());
            merge.build();
            return merge;
        } else if (mergeQuerys.size() == 1) {
            return mergeQuerys.get(0);
        } else {
            return query;
        }
    }

    private static boolean chooseJoinMergeJoinByRule(Map<String, Object> extraCmd) {
        return GeneralUtil.getExtraCmdBoolean(extraCmd, ConnectionProperties.JOIN_MERGE_JOIN_JUDGE_BY_RULE, true);
    }

    private static boolean chooseJoinMergeJoinForce(Map<String, Object> extraCmd) {
        return GeneralUtil.getExtraCmdBoolean(extraCmd, ConnectionProperties.JOIN_MERGE_JOIN, false);
    }

    private static boolean chooseShareNode(Map<String, Object> extraCmd) {
        return true;
    }

    private static class PartitionJoinResult {

        List<IBooleanFilter> joinFilters;
        String               joinGroup;
        boolean              broadcast = false;
        boolean              flag      = false; // 成功还是失败
    }

    /**
     * 找到join条件完全是分区键的filter，返回null代表没找到，否则返回join条件
     */
    private static PartitionJoinResult isJoinOnPartition(JoinNode join) {
        QueryTreeNode left = join.getLeftNode();
        QueryTreeNode right = join.getRightNode();

        PartitionJoinResult leftResult = isJoinOnPartitionOneSide(join.getLeftKeys(), left);
        PartitionJoinResult rightResult = isJoinOnPartitionOneSide(join.getRightKeys(), right);
        // 允许一张单表和一张广播表的组合
        if (!leftResult.flag && !rightResult.broadcast) {
            return leftResult;
        }

        if (!rightResult.flag && !leftResult.broadcast) {
            return rightResult;
        }

        PartitionJoinResult result = new PartitionJoinResult();
        result.broadcast = leftResult.broadcast || rightResult.broadcast;
        result.flag = StringUtils.equalsIgnoreCase(leftResult.joinGroup, rightResult.joinGroup) || result.broadcast;
        result.joinGroup = leftResult.joinGroup;
        result.joinFilters = join.getJoinFilter();
        return result;
    }

    /**
     * 判断insert..select的分区规则是否一致
     */
    private static PartitionJoinResult isInsertSelectOnPartition(QueryTreeNode left, QueryTreeNode right) {
        PartitionJoinResult leftResult = isJoinOnPartitionOneSide(new ArrayList<ISelectable>(), left);
        if (!leftResult.flag) {
            return leftResult;
        }

        PartitionJoinResult rightResult = isJoinOnPartitionOneSide(new ArrayList<ISelectable>(), right);
        if (!rightResult.flag) {
            return rightResult;
        }

        PartitionJoinResult result = new PartitionJoinResult();
        result.broadcast = leftResult.broadcast || rightResult.broadcast;
        result.flag = StringUtils.equalsIgnoreCase(leftResult.joinGroup, rightResult.joinGroup) || result.broadcast;
        result.joinGroup = leftResult.joinGroup;
        result.joinFilters = null;
        return result;
    }

    /**
     * 判断一个joinNode的左或则右节点的joinColumns是否和当前节点的分区键完全匹配
     */
    private static PartitionJoinResult isJoinOnPartitionOneSide(List<ISelectable> joinColumns, QueryTreeNode qtn) {
        PartitionJoinResult result = new PartitionJoinResult();
        // 递归拿左边的树
        if (qtn instanceof JoinNode) {
            result = isJoinOnPartition((JoinNode) qtn);
            if (!result.flag) {// 递归失败，直接返回
                result.flag = false;
                return result;
            }

            if (joinColumns.isEmpty()) { // 针对insert..select
                result.flag = true;
                return result;
            }

            List<IBooleanFilter> joinOnPatitionFilters = result.joinFilters;
            if (joinOnPatitionFilters == null) {// 底下不满足，直接退出
                result.flag = false;
                return result;
            } else {
                for (ISelectable joinColumn : joinColumns) {
                    ISelectable select = qtn.findColumn(joinColumn);
                    if (!isMatchJoinFilter(joinOnPatitionFilters, select)) {
                        result.flag = false;
                        return result;
                    }
                }

                result.flag = true;
                return result;
            }
        } else if (qtn instanceof QueryNode) {
            // 转一次为query中的select字段
            List<ISelectable> newJoinColumns = new LinkedList<ISelectable>();
            for (ISelectable joinColumn : joinColumns) {
                ISelectable newColumn = qtn.findColumn(joinColumn);
                if (newColumn == null) {
                    return null;
                } else {
                    newJoinColumns.add(newColumn);
                }
            }
            // 获取当前表的分区字段
            return isJoinOnPartitionOneSide(newJoinColumns, (QueryTreeNode) qtn.getChild());
        } else if (qtn instanceof MergeNode) {
            result.flag = false;
            return result; // 直接返回，不处理
        } else {
            String indexName = ((KVIndexNode) qtn).getKvIndexName();
            result.joinGroup = OptimizerContext.getContext().getRule().getJoinGroup(indexName);
            if (OptimizerContext.getContext().getRule().isBroadCast(((KVIndexNode) qtn).getKvIndexName())) {
                result.flag = true;
                result.broadcast = true;
                return result;
            }

            if (joinColumns.isEmpty()) { // 如果没有join列，可能是insert...select情况
                result.flag = true;
                return result;
            }

            // KVIndexNode
            List<String> shardColumns = OptimizerContext.getContext()
                .getRule()
                .getSharedColumns(((KVIndexNode) qtn).getKvIndexName());
            List<ColumnMeta> columns = new ArrayList<ColumnMeta>();
            TableMeta tableMeta = ((KVIndexNode) qtn).getTableMeta();
            for (String shardColumn : shardColumns) {
                ColumnMeta column = tableMeta.getColumn(shardColumn);
                if (column == null) {
                    // 可能是一个不存在的分库字段
                    result.flag = false;
                    return result;
                }

                columns.add(column);
            }

            String tableName = ((KVIndexNode) qtn).getTableName();
            if (qtn.getAlias() != null) {
                tableName = qtn.getAlias();
            }

            List<ISelectable> partitionColumns = OptimizerUtils.columnMetaListToIColumnList(columns, tableName);
            if (partitionColumns.isEmpty()) {
                result.flag = false;// 没有分库键
                return result;
            }

            // 要求joinColumns必须包含所有的partitionColumns
            for (ISelectable partitionColumn : partitionColumns) {
                boolean isFound = false;
                for (ISelectable joinColumn : joinColumns) {
                    if (joinColumn.getColumnName().equals(partitionColumn.getColumnName())) {// partition无别名
                        isFound = true;
                        break;
                    }
                }

                if (!isFound) {
                    result.flag = false;// 没有分库键
                    return result;
                }
            }

            result.flag = true;
            return result;
        }
    }

    /**
     * 判断是否是join条件中的一个字段，可能是左表或右边的字段
     */
    private static boolean isMatchJoinFilter(List<IBooleanFilter> joinFilters, ISelectable column) {
        for (IBooleanFilter joinFilter : joinFilters) {
            ISelectable leftJoinColumn = (ISelectable) joinFilter.getColumn();
            ISelectable rightJoinColumn = (ISelectable) joinFilter.getValue();

            if (leftJoinColumn.equals(column) || rightJoinColumn.equals(column)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 判断是否为单库单表 join 单库多表
     * 
     * @param left
     * @param right
     * @return
     */
    private static boolean isJoinOnOneGroup(QueryTreeNode left, QueryTreeNode right) {
        return isLeftJoinRightOnOneGroup(left, right) || isLeftJoinRightOnOneGroup(right, left);
    }

    /**
     * 左表是单库单表，右表为单库多表，是否满足join on group条件
     * 
     * @param left
     * @param right
     * @return
     */
    private static boolean isLeftJoinRightOnOneGroup(QueryTreeNode left, QueryTreeNode right) {
        if (left instanceof MergeNode) {// 左表不是merge
            return false;
        }

        if (!(right instanceof MergeNode)) {// 右表是merge
            return false;
        }

        String group1 = getOneGroup(left);
        String group2 = getOneGroup(right);
        if (group1 != null && group2 != null) {
            if (group1.equals(group2)) {
                left.setBroadcast(true); // 将左表设置为广播表
                return true;
            }
        }

        return false;
    }

    private static String getOneGroup(QueryTreeNode qtn) {
        String group = qtn.getDataNode();
        for (ASTNode node : qtn.getChildren()) {
            if (node instanceof MergeNode) {
                return null;
            }

            if (!group.equals(node.getDataNode())) {
                return null;
            }
        }

        return group;
    }

    private static QueryTreeNode buildJoinMergeJoin(JoinNode join, QueryTreeNode left, QueryTreeNode right,
                                                    Map<String, Object> extraCmd) {
        // 根据表名的后缀来判断两个表是不是对应的表
        // 底下的节点已经被优先处理
        // 1. 如果是KvIndexNode，没必要转化为join merge join，直接join即可
        // 2. 如果是QueryNode，底下已经将其转化为merge下套query+child
        // 所以，只要处理MergeNode即可
        Map<String, QueryTreeNode> leftIdentifierExtras = new HashMap<String, QueryTreeNode>();
        List<JoinNode> joins = new ArrayList();
        List<QueryTreeNode> rights = new ArrayList();
        List<QueryTreeNode> lefts = new ArrayList();
        boolean leftBroadCast = false;
        boolean rightBroadCast = false;
        if (left instanceof MergeNode) {
            for (ASTNode child : left.getChildren()) {
                leftIdentifierExtras.put((String) child.getExtra(), (QueryTreeNode) child);
                lefts.add((QueryTreeNode) child);
            }
        } else {
            // 可能是广播表
            leftBroadCast = left.isBroadcast();
        }

        if (right instanceof MergeNode) {
            for (ASTNode child : right.getChildren()) {
                rights.add((QueryTreeNode) child);
            }
        } else {
            // 可能是广播表
            rightBroadCast = right.isBroadcast();
        }

        // 非广播表，并且存在聚合计算，不能展开
        if (!leftBroadCast && (left.isExistAggregate() || left.getLimitFrom() != null || left.getLimitTo() != null)) {
            return null;
        }

        // 非广播表，并且存在聚合计算，不能展开
        if (!rightBroadCast && (right.isExistAggregate() || right.getLimitFrom() != null || right.getLimitTo() != null)) {
            return null;
        }

        if (leftBroadCast && rightBroadCast) {
            return join;// 两个广播表之间的join，直接返回
        } else if (leftBroadCast) {// 左边是广播表
            for (QueryTreeNode r : rights) {
                JoinNode newj = join.copySelf();
                QueryTreeNode newL = left.copy();
                setExecuteOn(newL, r.getDataNode());// 广播表的执行节点跟着右边走
                newj.setLeftNode(newL);
                newj.setRightNode(r);
                newj.executeOn(r.getDataNode());
                newj.setExtra(r.getExtra());
                joins.add(newj);
            }
        } else if (rightBroadCast) {
            for (QueryTreeNode l : lefts) {
                JoinNode newj = join.copySelf();
                QueryTreeNode newR = right.copy();
                setExecuteOn(newR, l.getDataNode());// 广播表的执行节点跟着右边走
                newj.setLeftNode(l);
                newj.setRightNode(newR);
                newj.executeOn(l.getDataNode());
                newj.setExtra(l.getExtra());
                joins.add(newj);
            }
        } else {
            // 根据后缀，找到匹配的表，生成join
            for (QueryTreeNode r : rights) {
                QueryTreeNode l = leftIdentifierExtras.remove(r.getExtra());
                if (l == null) {
                    return null; // 转化失败，直接退回merge join merge的处理
                }

                JoinNode newj = join.copySelf();
                newj.setLeftNode(l);
                newj.setRightNode(r);
                newj.executeOn(l.getDataNode());
                newj.setExtra(l.getExtra()); // 因为left/right的extra相同，只要选择一个即可
                joins.add(newj);
            }
        }

        if (joins.size() > 1) {
            MergeNode merge = new MergeNode();
            for (JoinNode j : joins) {
                merge.merge(j);
            }

            merge.setSubqueryOnFilterId(joins.get(0).getSubqueryOnFilterId());
            merge.executeOn(joins.get(0).getDataNode()).setExtra(joins.get(0).getExtra());
            merge.build();
            return merge;
        } else if (joins.size() == 1) {
            return joins.get(0);
        }

        return null;
    }

    /**
     * 将insert...select的节点构造为merge
     * 
     * <pre>
     * 注意点：
     * 1. 针对join group相同而分区不同的，则相信配置是正确的，不会再做left/right分区是否一致的check
     * 2. insert ... select xx where id = 3，构建出来的insert是一个全表扫描，而select会是id=3的分区，分区条件也不会是一致
     */
    private static ASTNode buildMergeInsertSelect(DMLNode dne, QueryTreeNode left, QueryTreeNode right,
                                                  Map<String, Object> extraCmd) {
        // 根据表名的后缀来判断两个表是不是对应的表
        // 底下的节点已经被优先处理
        // 1. 如果是KvIndexNode，没必要转化为, 直接返回 即可
        // 2. 如果是QueryNode，底下已经将其转化为merge下套query+child
        // 所以，只要处理MergeNode即可

        // 因为insert...select，分库条件只在select中存在，所以需要以select为驱动表
        // 如果insert中分区表在本次select中不存在，则忽略当前insert的分区表
        // 也就是insert ... select xx where id = 3, 只会构建出一个id=3分区下的insert select节点.
        Map<String, QueryTreeNode> rightIdentifierExtras = new HashMap<String, QueryTreeNode>();
        List<DMLNode> inserts = new ArrayList();
        List<QueryTreeNode> rights = new ArrayList();
        List<QueryTreeNode> lefts = new ArrayList();
        boolean leftBroadCast = false;
        boolean rightBroadCast = false;
        if (left instanceof MergeNode) {
            for (ASTNode child : left.getChildren()) {
                lefts.add((QueryTreeNode) child);
            }
        } else {
            // 可能是广播表
            leftBroadCast = left.isBroadcast();
        }

        if (right instanceof MergeNode) {
            for (ASTNode child : right.getChildren()) {
                rightIdentifierExtras.put((String) child.getExtra(), (QueryTreeNode) child);
                rights.add((QueryTreeNode) child);
            }
        } else {
            // 可能是广播表
            rightBroadCast = right.isBroadcast();
        }

        // 非广播表，并且存在聚合计算，不能展开
        if (!leftBroadCast && (left.isExistAggregate() || left.getLimitFrom() != null || left.getLimitTo() != null)) {
            return null;
        }

        // 非广播表，并且存在聚合计算，不能展开
        if (!rightBroadCast && (right.isExistAggregate() || right.getLimitFrom() != null || right.getLimitTo() != null)) {
            return null;
        }

        if (leftBroadCast && rightBroadCast) {
            return dne;// 两个广播表之间的insert select，直接返回
        } else if (leftBroadCast) {// 左边是广播表，会在每个分表上执行，然后广播表数据互相merge
            for (QueryTreeNode r : rights) {
                DMLNode newj = dne.copySelf();
                QueryTreeNode newL = left.copy();
                setExecuteOn(newL, r.getDataNode());// 广播表的执行节点跟着右边走
                newj.setNode((TableNode) newL);
                newj.setSelectNode(r);
                newj.executeOn(r.getDataNode());
                newj.setExtra(r.getExtra());
                inserts.add(newj);
            }
        } else if (rightBroadCast) {
            for (QueryTreeNode l : lefts) {
                DMLNode newj = dne.copySelf();
                QueryTreeNode newR = right.copy();
                setExecuteOn(newR, l.getDataNode());// 广播表的执行节点跟着右边走
                newj.setNode((TableNode) l);
                newj.setSelectNode(newR);
                newj.executeOn(l.getDataNode());
                newj.setExtra(l.getExtra());
                inserts.add(newj);
            }
        } else {
            // 根据后缀，找到匹配的表，生成insert select
            for (QueryTreeNode l : lefts) {
                QueryTreeNode r = rightIdentifierExtras.remove(l.getExtra());
                if (r == null) {
                    continue; // 如果select中没有，则忽略当前insert，注意不是返回null
                }

                DMLNode newj = dne.copySelf();
                newj.setNode((TableNode) l);
                newj.setSelectNode(r);
                newj.executeOn(l.getDataNode());
                newj.setExtra(l.getExtra()); // 因为left/right的extra相同，只要选择一个即可
                inserts.add(newj);
            }
        }

        if (inserts.size() > 1) {
            MergeNode merge = new MergeNode();
            for (DMLNode j : inserts) {
                merge.merge(j);
            }

            merge.executeOn(inserts.get(0).getDataNode()).setExtra(inserts.get(0).getExtra());
            merge.build();
            return merge;
        } else if (inserts.size() == 1) {
            return inserts.get(0);
        }

        return null;
    }

    /**
     * 递归设置executeOn
     */
    private static void setExecuteOn(DMLNode dne, String dataNode) {
        QueryTreeNode qtn = dne.getNode();
        if (qtn != null) {
            setExecuteOn(qtn, dataNode);
        }

        qtn = dne.getSelectNode();
        if (qtn != null) {
            setExecuteOn(qtn, dataNode);
        }
    }

    /**
     * 递归设置executeOn
     */
    private static void setExecuteOn(QueryTreeNode qtn, String dataNode) {
        for (ASTNode node : qtn.getChildren()) {
            setExecuteOn((QueryTreeNode) node, dataNode);
        }

        qtn.executeOn(dataNode);
    }

    /**
     * 根据表名提取唯一标识
     * 
     * <pre>
     * 1. tddl中的分库分表时，比如分16个库，每个库128张表，总共1024张表. 表的顺序为递增，从0000-1023，
     *    此时executeNode就是库名，表名可通过后缀获取，两者结合可以唯一确定一张表
     * 2. cobar中的分库分表，只会分库，不分表，每个库中的表名都一样. 
     *    此时executeNode就是库名，已经可以唯一确定一张表
     * </pre>
     */
    private static String getIdentifierExtra(KVIndexNode child, int shareIndex) {
        String tableName = child.getActualTableName(shareIndex);
        if (tableName == null) {
            tableName = child.getIndexName();
        }

        Matcher matcher = suffixPattern.matcher(tableName);
        if (matcher.find()) {
            return (child.getDataNode(shareIndex) + "_" + matcher.group());
        } else {
            return child.getDataNode(shareIndex);
        }
    }

    private static IFilter createFilter(List<ISelectable> columns, List<Object> values) {
        IFilter insertFilter = null;
        if (columns.size() == 1) {
            IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
            f.setOperation(OPERATION.EQ);
            f.setColumn(columns.get(0));
            f.setValue(values.get(0));
            insertFilter = f;
        } else {
            ILogicalFilter and = ASTNodeFactory.getInstance().createLogicalFilter();
            and.setOperation(OPERATION.AND);
            for (int i = 0; i < columns.size(); i++) {
                Comparable c = columns.get(i);
                IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
                f.setOperation(OPERATION.EQ);
                f.setColumn(c);
                f.setValue(values.get(i));
                and.addSubFilter(f);
            }

            insertFilter = and;
        }
        return insertFilter;
    }

}
