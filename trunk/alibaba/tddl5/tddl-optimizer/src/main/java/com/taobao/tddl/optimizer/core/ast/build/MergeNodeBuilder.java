package com.taobao.tddl.optimizer.core.ast.build;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * @author Dreamond
 * @author jianghang 2013-11-8 下午2:33:51
 * @since 5.0.0
 */
public class MergeNodeBuilder extends QueryTreeNodeBuilder {

    public MergeNodeBuilder(MergeNode mergeNode){
        this.setNode(mergeNode);
    }

    @Override
    public MergeNode getNode() {
        return (MergeNode) super.getNode();
    }

    @Override
    public void build() {
        for (ASTNode sub : this.getNode().getChildren()) {
            sub.build();
        }

        this.buildExistSequenceVal();
        if (!(this.getNode().getChild() instanceof QueryTreeNode)) {
            return;
        }
        this.buildAlias();
        this.buildSelected();
        this.buildWhere();
        this.buildGroupBy();
        this.buildOrderBy();
        this.buildHaving();
        this.buildFunction();
        this.buildExistAggregate();
        // 最后一个处理
        this.buildLimit();
    }

    /**
     * <pre>
     * 1. max(id)+min(id)，要把聚合函数单独推到子节点去(比如max(id),min(id))，然后在父节点留一个scalar函数进行计算
     * 2. substring(),to_date()等简单scalar函数(不包含条件1的函数)，推到子节点去，然后在父节点留一个字段，而不是函数
     * </pre>
     */
    @Override
    public void buildFunction() {
        super.buildFunction();

        List<IFunction> aggregateInScalar = new ArrayList<IFunction>();
        List<ISelectable> simpleScalar = new ArrayList<ISelectable>();
        for (ISelectable s : this.getNode().getColumnsSelected()) {
            if (s instanceof IFunction) {
                if (IFunction.FunctionType.Aggregate.equals(((IFunction) s).getFunctionType())) {
                    continue;
                } else {
                    List<IFunction> argsAggregateFunctions = this.findAggregateFunctionsInScalar((IFunction) s);
                    if (!argsAggregateFunctions.isEmpty()) {
                        aggregateInScalar.addAll(argsAggregateFunctions);
                    } else {
                        simpleScalar.add(s);
                    }
                }
            }
        }

        // case 2，删除父节点的简单scalar函数
        // this.getNode().getColumnsSelected().removeAll(simpleScalar);
        // for (ISelectable f : simpleScalar) {
        // IColumn scalarColumn = ASTNodeFactory.getInstance().createColumn();
        // if (f.getAlias() != null) {
        // scalarColumn.setColumnName(f.getAlias());
        // }
        // scalarColumn.setTableName(f.getTableName());
        // scalarColumn.setDistinct(f.isDistinct());
        // scalarColumn.setIsNot(f.isNot());
        // this.getNode().addColumnsSelected(buildSelectable(scalarColumn,
        // true));
        // }

        List<ISelectable> toRemove = new ArrayList();
        for (ISelectable s : ((QueryTreeNode) this.getNode().getChild()).getColumnsSelected()) {
            if (s instanceof IFunction && IFunction.FunctionType.Scalar.equals(((IFunction) s).getFunctionType())) {
                if (!this.findAggregateFunctionsInScalar((IFunction) s).isEmpty()) { // scalar里存在聚合函数
                    toRemove.add(s);
                }
            }
        }

        for (ASTNode child : this.getNode().getChildren()) {
            ((QueryTreeNode) child).getColumnsSelected().removeAll(toRemove);// 干掉查询的min(id)+max(id)函数
            for (ISelectable f : aggregateInScalar) {
                // 只添加min(id) ,max(id)的独立函数
                ((QueryTreeNode) child).addColumnsSelected(f.copy());
            }

            child.build();
        }
    }

    @Override
    public void buildFunction(IFunction f, boolean findInSelectList) {
        if (FunctionType.Aggregate == f.getFunctionType()) {
            setExistAggregate();
        }

        for (Object arg : f.getArgs()) {
            if (arg instanceof IFunction) {
                this.buildSelectable((ISelectable) arg, findInSelectList);
            } else if (!this.getNode().isDistinctByShardColumns() && arg instanceof ISelectable) {
                // 针对非distinct的函数，没必要下推字段
                if (((ISelectable) arg).isDistinct()) {
                    this.buildSelectable((ISelectable) arg, findInSelectList);
                }
            }
        }
    }

    private List<IFunction> findAggregateFunctionsInScalar(IFunction s) {
        List<IFunction> res = new ArrayList();
        this.findAggregateFunctionsInScalar(s, res);
        return res;
    }

    private void findAggregateFunctionsInScalar(IFunction s, List<IFunction> res) {
        if (IFunction.FunctionType.Aggregate.equals(s.getFunctionType())) {
            res.add(s);
        }

        for (Object arg : s.getArgs()) {
            if (arg instanceof IFunction) {
                this.findAggregateFunctionsInScalar((IFunction) arg, res);
            }
        }
    }

    @Override
    public void buildHaving() {
        if (this.getNode().isGroupByShardColumns()) {
            // 如果是groupBy分库键，having条件直接底下算
            this.getNode().having("");
        } else if (this.getNode().getHavingFilter() == null) {
            if (this.getNode().getChild() instanceof QueryTreeNode) {
                IFilter havingFilter = ((QueryTreeNode) this.getNode().getChild()).getHavingFilter();
                if (havingFilter != null) {
                    this.getNode().having(OptimizerUtils.copyFilter(havingFilter));
                    replaceAliasInFilter(this.getNode().getHavingFilter(),
                        ((QueryTreeNode) this.getNode().getChild()).getAlias());
                    // 干掉子节点查询的having条件，转移到父节点中 【暂时不清理，交由OrderByPusher来判断】
                    // for (ASTNode child : this.getNode().getChildren()) {
                    // if (child instanceof QueryTreeNode) {
                    // ((QueryTreeNode) child).having("");
                    // }
                    // }
                }
            }
        }
    }

    private void replaceAliasInFilter(Object filter, String alias) {
        if (filter instanceof IFunction) {
            for (Object sub : ((IFunction) filter).getArgs()) {
                this.replaceAliasInFilter(sub, alias);
            }
        }

        if (filter instanceof ISelectable) {
            if (alias != null) {
                ((ISelectable) filter).setTableName(alias);
            }

            if (((ISelectable) filter).getAlias() != null) {
                ((ISelectable) filter).setColumnName(((ISelectable) filter).getAlias());
                ((ISelectable) filter).setAlias(null);
            }
        }

    }

    public void buildLimit() {
        if (this.getNode().getLimitFrom() == null && this.getNode().getLimitTo() == null) {

            // 将子节点的limit条件转移到父节点
            // 不能出现多级的merge节点都带着limit
            Comparable from = ((QueryTreeNode) this.getNode().getChild()).getLimitFrom();
            Comparable to = ((QueryTreeNode) this.getNode().getChild()).getLimitTo();

            if ((from instanceof IBindVal && ((IBindVal) from).getValue() != null)
                || (to instanceof IBindVal && ((IBindVal) to).getValue() != null)) {
                // 说明是batch中针对limit使用绑定变量，暂时不支持
                throw new NotSupportException("batch中不支持对分库分表limit的绑定变量");
            }

            this.getNode().setLimitFrom(from);
            this.getNode().setLimitTo(to);

            if (from instanceof Long && to instanceof Long) {
                if ((from != null && (Long) from != -1) || (to != null && (Long) to != -1)) {
                    for (ASTNode s : this.getNode().getChildren()) {
                        if (node.isExistAggregate()) {
                            // 如果节点存在聚合函数，limit不能下推，干掉子节点的limit，由父节点进行计算
                            ((QueryTreeNode) s).setLimitFrom(null);
                            ((QueryTreeNode) s).setLimitTo(null);
                        } else {
                            // 底下采取limit 0,from+to逻辑，上层来过滤
                            ((QueryTreeNode) s).setLimitFrom(0L);
                            ((QueryTreeNode) s).setLimitTo((Long) from + (Long) to);
                        }
                    }
                }
            }
        }

    }

    @Override
    public void pusherFunction(IFunction f) {
        // 下推select
        // TODO 需要判断：如果having中存在函数count(JID)，但对应的JID为select中的列，此时不能下推
        for (int i = 0; i < this.getNode().getChildren().size(); i++) {
            QueryTreeNode sub = (QueryTreeNode) this.getNode().getChildren().get(i);
            if (this.findColumnFromOtherNode(f, sub) == null) {
                ISelectable copyf = f.copy();
                sub.addColumnsSelected(copyf); // 查找一下子节点的select中是已包含对应函数列
                sub.getBuilder().buildSelectable(copyf, true);// 单独build这一函数列
            }
        }
    }

    @Override
    public void buildOrderBy() {
        // 如果merge本身没指定order by，则继承子节点的order by
        if (this.getNode().getOrderBys() == null || this.getNode().getOrderBys().isEmpty()) {
            QueryTreeNode child = (QueryTreeNode) this.getNode().getChild();
            if (child.getOrderBys() != null) {
                for (IOrderBy o : child.getOrderBys()) {
                    ISelectable sc = o.getColumn().copy();
                    if (o.getColumn().getAlias() != null) {
                        sc.setColumnName(o.getColumn().getAlias());
                    }
                    if (child.getAlias() != null) {
                        sc.setTableName(child.getAlias());
                    }
                    this.getNode().orderBy(sc, o.getDirection());
                }
            }
        }

        super.buildOrderBy();

        // 检查group列是否在select列中
        for (IOrderBy order : node.getOrderBys()) {
            checkOrderColumnExist(order, (QueryTreeNode) node.getChild());
        }
    }

    @Override
    public void buildGroupBy() {
        // 如果merge本身没指定group by，则继承子节点的group by
        if (this.getNode().getGroupBys() == null || this.getNode().getGroupBys().isEmpty()) {
            QueryTreeNode child = (QueryTreeNode) this.getNode().getChild();
            if (child.getGroupBys() != null) {
                for (IOrderBy s : child.getGroupBys()) {
                    IOrderBy sc = s.copy();
                    if (s.getAlias() != null) {
                        sc.setColumnName(s.getAlias());
                    }
                    if (child.getAlias() != null) {
                        sc.setTableName(child.getAlias());
                    }
                    this.getNode().groupBy(sc);
                }
            }
        }

        super.buildGroupBy();

        // 检查group列是否在select列中
        for (IOrderBy order : node.getGroupBys()) {
            checkOrderColumnExist(order, (QueryTreeNode) node.getChild());
        }
    }

    private void buildAlias() {
        if (this.getNode().getAlias() == null) {
            this.getNode().alias(((QueryTreeNode) this.getNode().getChild()).getAlias());
        }
    }

    /**
     * 构建列信息
     * 
     * @param indexNode
     */
    public void buildSelected() {
        buildSelectedFromSelectableObject();
    }

    private void buildSelectedFromSelectableObject() {
        if (this.getNode().getColumnsSelected().isEmpty()) {
            this.getNode()
                .getColumnsSelected()
                .add(ASTNodeFactory.getInstance().createColumn().setColumnName(IColumn.STAR));
        }
        // 如果有 * ，最后需要把*删掉
        List<ISelectable> delete = new LinkedList();

        for (ISelectable selected : getNode().getColumnsSelected()) {
            if (selected.getColumnName().equals(IColumn.STAR)) {
                delete.add(selected);
            }
        }
        if (!delete.isEmpty()) {
            this.getNode().getColumnsSelected().removeAll(delete);
        }

        for (ISelectable selected : delete) {
            // 遇到*就把所有列再添加一遍
            // select *,id这样的语法最后会有两个id列，mysql是这样的
            QueryTreeNode child = (QueryTreeNode) this.getNode().getChild();
            for (ISelectable selectedFromChild : child.getColumnsSelected()) {
                if (selected.getTableName() != null) {
                    if (!selected.getTableName().equals(selectedFromChild.getTableName())) {
                        break;
                    }
                }

                ISelectable newS = selectedFromChild.copy();
                // IColumn newS = ASTNodeFactory.getInstance().createColumn();

                if (child.getAlias() != null) {
                    newS.setTableName(child.getAlias());
                } else {
                    newS.setTableName(selectedFromChild.getTableName());
                }

                if (selectedFromChild.getAlias() == null) {
                    newS.setColumnName(selectedFromChild.getColumnName());
                } else {
                    newS.setColumnName(selectedFromChild.getAlias());
                }

                getNode().getColumnsSelected().add(newS);
            }
        }

        for (int i = 0; i < getNode().getColumnsSelected().size(); i++) {
            getNode().getColumnsSelected().set(i, this.buildSelectable(getNode().getColumnsSelected().get(i)));
        }
    }

    @Override
    public ISelectable getSelectableFromChild(ISelectable c) {
        QueryTreeNode child = (QueryTreeNode) this.getNode().getChild();
        if (IColumn.STAR.equals(c.getColumnName())) {
            return c;
        }

        if (c instanceof IFunction) {
            return c;
        }

        if (child.hasColumn(c)) {
            ISelectable s = this.getColumnFromOtherNode(c, child);
            if (s == null) {
                // 下推select
                for (int i = 0; i < this.getNode().getChildren().size(); i++) {
                    QueryTreeNode sub = (QueryTreeNode) this.getNode().getChildren().get(i);
                    sub.addColumnsSelected(c.copy());
                }
                s = this.getColumnFromOtherNode(c, child);
            }
            return s;
        } else {
            return null;
        }
    }

}
