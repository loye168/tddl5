package com.taobao.tddl.optimizer.core.ast.build;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * @author Dreamond
 * @author jianghang 2013-11-8 下午2:33:51
 * @since 5.0.0
 */
public class JoinNodeBuilder extends QueryTreeNodeBuilder {

    public JoinNodeBuilder(JoinNode joinNode){
        this.setNode(joinNode);
    }

    @Override
    public JoinNode getNode() {
        return (JoinNode) super.getNode();
    }

    @Override
    public void build() {
        if (this.getNode().isUedForIndexJoinPK()) {
            return;
        }
        this.buildSelected();
        this.buildJoinKeys();
        this.buildWhere();
        this.buildGroupBy();
        this.buildOrderBy();
        this.buildHaving();
        this.buildColumnRefered();
        this.buildExistAggregate();
        this.buildExistSequenceVal();
    }

    private void buildColumnRefered() {
        List<ISelectable> columnRefered = new ArrayList();
        columnRefered.addAll(this.getNode().getColumnsSelected());

        for (IBooleanFilter f : this.getNode().getJoinFilter()) {
            ISelectable left = (ISelectable) f.getColumn();
            ISelectable right = (ISelectable) f.getValue();

            if (!columnRefered.contains(left)) {
                columnRefered.add(left);
            }

            if (!columnRefered.contains(right)) {
                columnRefered.add(right);
            }
        }

    }

    private void buildJoinKeys() {
        if (this.getNode().isUedForIndexJoinPK()) {
            return;
        }
        List<IBooleanFilter> otherJoinOnFilters = new ArrayList(this.getNode().getJoinFilter().size());
        for (IBooleanFilter f : this.getNode().getJoinFilter()) {
            ISelectable leftKey = null;
            if (f.getColumn() != null && f.getColumn() instanceof ISelectable) {
                leftKey = this.getColumnFromOtherNode((ISelectable) f.getColumn(), this.getNode().getLeftNode());
            }

            ISelectable rightKey = null;
            if (f.getValue() != null && f.getValue() instanceof ISelectable) {
                rightKey = this.getColumnFromOtherNode((ISelectable) f.getValue(), this.getNode().getRightNode());
            }

            // 可能顺序调换了，重新找一次
            if (leftKey == null || rightKey == null) {
                if (f.getValue() != null && f.getValue() instanceof ISelectable) {
                    leftKey = this.getColumnFromOtherNode((ISelectable) f.getValue(), this.getNode().getLeftNode());
                }

                if (f.getColumn() != null && f.getColumn() instanceof ISelectable) {
                    rightKey = this.getColumnFromOtherNode((ISelectable) f.getColumn(), this.getNode().getRightNode());
                }
            }

            if (leftKey == null || rightKey == null) {
                // 可能有以下情况
                // id=1,s.id=s.key_id
                IFilter otherJoinOnFilter = this.getNode().getOtherJoinOnFilter();
                otherJoinOnFilter = FilterUtils.and(otherJoinOnFilter, f);
                this.getNode().setOtherJoinOnFilter(otherJoinOnFilter);
                otherJoinOnFilters.add(f);
                continue;
            }

            /**
             * 如果是回表操作，不能把索引的joinKey添加到temp中，否则如果有merge，这个列会被加到sql的select中，
             * 而导致找不到列
             */
            if (!this.getNode().isUedForIndexJoinPK()) {
                f.setColumn(buildSelectable(leftKey));
                f.setValue(buildSelectable(rightKey));
            }

        }

        this.getNode().getJoinFilter().removeAll(otherJoinOnFilters);
        this.buildFilter(this.getNode().getOtherJoinOnFilter(), false);
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
        // 如果为空，代表是用*
        if (this.getNode().getColumnsSelected().isEmpty()) {
            this.getNode()
                .getColumnsSelected()
                .add(ASTNodeFactory.getInstance().createColumn().setColumnName(IColumn.STAR));
        }
        // 如果有 * ，最后需要把*删掉
        List<Integer> delete = new LinkedList();

        int index = 0;
        for (ISelectable selected : getNode().getColumnsSelected()) {
            if (IColumn.STAR.equals(selected.getColumnName())) {
                delete.add(index);
                break;
            }
        }
        if (!delete.isEmpty()) {
            List<ISelectable> columnsWithOutStar = new ArrayList();

            for (int i = 0; i < this.getNode().getColumnsSelected().size(); i++) {

                ISelectable selected = this.getNode().getColumnsSelected().get(i);
                if (this.getNode().getColumnsSelected().get(i).getColumnName().equals(IColumn.STAR)) {
                    // 遇到*就把所有列再添加一遍
                    // select *,id这样的语法最后会有两个id列，mysql是这样的
                    for (ASTNode child : this.getNode().getChildren()) {
                        for (ISelectable selectedFromChild : ((QueryTreeNode) child).getColumnsSelectedForParent()) {
                            // 考虑
                            // a. SELECT B.* FROM TABLE1 A INNER JOIN TABLE2 B
                            // b. SELECT * FROM TABLE1 A INNER JOIN TABLE2 B
                            if (selected.getTableName() != null
                                && !selected.getTableName().equals(selectedFromChild.getTableName())) {
                                continue;
                            }

                            IColumn newS = ASTNodeFactory.getInstance().createColumn();
                            // 尝试复制子节点的表别名
                            if (((QueryTreeNode) child).getAlias() != null) {
                                newS.setTableName(((QueryTreeNode) child).getAlias());
                            } else {
                                newS.setTableName(selectedFromChild.getTableName());
                            }

                            if (selectedFromChild.getAlias() != null) {
                                newS.setColumnName(selectedFromChild.getAlias());
                            } else {
                                newS.setColumnName(selectedFromChild.getColumnName());
                            }

                            columnsWithOutStar.add(newS);
                        }
                    }
                } else {
                    columnsWithOutStar.add(this.getNode().getColumnsSelected().get(i));
                }
            }

            this.getNode().select(columnsWithOutStar);
        }

        for (int i = 0; i < getNode().getColumnsSelected().size(); i++) {
            getNode().getColumnsSelected().set(i, this.buildSelectable(getNode().getColumnsSelected().get(i)));
        }

    }

    @Override
    public ISelectable getSelectableFromChild(ISelectable c) {
        if (c instanceof IFunction) {
            return c;
        }

        if (IColumn.STAR.equals(c.getColumnName())) {
            return c;
        }

        QueryTreeNode left = this.getNode().getLeftNode();
        QueryTreeNode right = this.getNode().getRightNode();
        ISelectable resFromLeft = null;
        ISelectable resFromRight = null;
        boolean isLeft = left.hasColumn(c);
        boolean isRight = right.hasColumn(c);
        if (isLeft && isRight) {
            throw new IllegalArgumentException("Column '" + c.getColumnName() + "' is ambiguous in JoinNode");
        }

        if (isLeft) {// 可能在select/from中
            resFromLeft = this.getColumnFromOtherNode(c, left);
            if (resFromLeft == null) {// 如果不在select中，添加到select进行join传递
                left.addColumnsSelected(c.copy());
                resFromLeft = this.getColumnFromOtherNode(c, left);
            }
        }

        if (isRight) {// 可能在select/from中
            resFromRight = this.getColumnFromOtherNode(c, right);
            if (resFromRight == null) {// 如果不在select中，添加到select进行join传递
                right.addColumnsSelected(c.copy());
                resFromRight = this.getColumnFromOtherNode(c, right);
            }
        }

        return resFromLeft == null ? resFromRight : resFromLeft;
    }

    public void buildFunction(IFunction f) {
        if (f.getArgs().size() == 0) {
            return;
        }

        List<Object> args = f.getArgs();
        for (int i = 0; i < args.size(); i++) {
            if (args.get(i) instanceof ISelectable) {
                args.set(i, this.buildSelectable((ISelectable) args.get(i)));
            }
        }
    }

    public void pusherFunction(IFunction f) {

    }

    @Override
    protected void buildOrderBy() {
        super.buildOrderBy();

        // 如果子节点出现merge,说明不可下推,需要自己做计算
        if (this.getNode().getLeftNode() instanceof MergeNode || this.getNode().getRightNode() instanceof MergeNode) {
            // 检查group列是否在select列中
            for (IOrderBy order : node.getOrderBys()) {
                checkOrderColumnExist(order, node);
            }
        }
    }

    @Override
    protected void buildGroupBy() {
        super.buildGroupBy();

        // 如果子节点出现merge,说明不可下推,需要自己做计算
        if (this.getNode().getLeftNode() instanceof MergeNode || this.getNode().getRightNode() instanceof MergeNode) {
            // 检查group列是否在select列中
            for (IOrderBy order : node.getGroupBys()) {
                checkOrderColumnExist(order, node);
            }
        }
    }

}
