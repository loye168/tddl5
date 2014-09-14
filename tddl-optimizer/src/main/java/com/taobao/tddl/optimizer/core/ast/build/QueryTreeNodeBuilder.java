package com.taobao.tddl.optimizer.core.ast.build;

import java.util.List;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.IGroupFilter;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISequenceVal;
import com.taobao.tddl.optimizer.exception.OptimizerException;
import com.taobao.tddl.optimizer.utils.UniqIdGen;

/**
 * @author Dreamond
 * @author jianghang 2013-11-8 下午2:33:51
 * @since 5.0.0
 */
public abstract class QueryTreeNodeBuilder {

    protected QueryTreeNode node;

    public QueryTreeNodeBuilder(){
    }

    public QueryTreeNode getNode() {
        return node;
    }

    public void setNode(QueryTreeNode node) {
        this.node = node;
    }

    public abstract void build();

    public ISelectable buildSelectable(ISelectable c) {
        return this.buildSelectable(c, false);
    }

    /**
     * 用于标记当前节点是否需要根据meta信息填充信息
     * 
     * <pre>
     * SQL. 
     *  a. select id + 2 as id , id from test where id = 2 having id = 4;
     *  b. select id + 2 as id , id from test where id = 2 order by count(id)
     * 
     * 解释：
     * 1.  COLUMN/WHERE/JOIN中列，是取自FROM的表字段
     * 2.  HAVING/ORDER BY/GROUP BY中的列，是取自SELECT中返回的字段，获取对应别名数据
     * </pre>
     * 
     * @param c
     * @param findInSelectList 如果在from的meta中找不到，是否继续在select中寻找
     * @return
     */
    public ISelectable buildSelectable(ISelectable c, boolean findInSelectList) {
        if (c == null) {
            return null;
        }

        if (c.getCorrelateOnFilterId() != null && c.getCorrelateOnFilterId() > 0L) {
            this.node.addColumnsCorrelate(c);
            return c;
        }

        // 比如SELECT A.ID FROM TABLE1 A，将A.ID改名为TABLE1.ID
        if (c.getTableName() != null) {
            // 对于TableNode如果别名存在别名
            if (node instanceof TableNode && (!(node instanceof KVIndexNode))) {
                boolean isSameName = c.getTableName().equals(node.getAlias())
                                     || c.getTableName().equals(((TableNode) node).getTableName());
                if (node.isSubQuery() && node.getSubAlias() != null) {
                    isSameName |= c.getTableName().equals(node.getSubAlias());
                }

                if (!isSameName) {
                    if (node.getParent() == null) {
                        throw new IllegalArgumentException("column: " + c.getFullName() + " is not existed in either "
                                                           + this.getNode().getName() + " or select clause");
                    }
                } else {
                    c.setTableName(((TableNode) node).getTableName());// 统一改为表名
                }
            }
        }

        ISelectable column = null;
        ISelectable columnFromMeta = null;

        if (findInSelectList) { // 优先查找select
            ISelectable columnFromSelected = getColumnFromSelecteList(c);
            if (columnFromSelected != null) {
                column = columnFromSelected;
                // 在select中找到了一次后，下次不能再从select中，遇到MAX(ID) AS ID会陷入死循环
                // 但遇到count(JID)查找时，JID为select中的列，需要再次从select中
                // if (c instanceof IColumn) {
                findInSelectList = false;
                // }
            }
        }

        boolean isCorrelate = false;
        if (column == null) {// 查找table meta
            columnFromMeta = this.getSelectableFromChild(c);
            if (columnFromMeta == null) {
                columnFromMeta = getSelectableFromParent(c);
                if (columnFromMeta != null) {
                    isCorrelate = true;
                }
            }

            if (columnFromMeta != null) {
                column = columnFromMeta;
                // 直接从子类的table定义中获取表字段，然后根据当前column状态，设置alias和distinct
                column.setAlias(c.getAlias());
                column.setDistinct(c.isDistinct());
            }
        }

        if (isCorrelate) {
            this.node.setCorrelatedSubquery(true);
            // 替换为correlated val
            if (column.getCorrelateOnFilterId().equals(0L)) {
                column.setCorrelateOnFilterId(UniqIdGen.genSubqueryID());
            }

            this.node.addColumnsCorrelate(column);
        }

        if (column == null) {
            throw new IllegalArgumentException("column: " + c.getFullName() + " is not existed in either "
                                               + this.getNode().getName() + " or select clause");
        }

        if ((column instanceof IColumn) && !IColumn.STAR.equals(column.getColumnName())) {
            node.addColumnsRefered(column); // refered不需要重复字段,select添加允许重复
            if (column.isDistinct()) {
                setExistAggregate();
            }
        }

        if (column instanceof IFunction) {
            if (isSubqueryFunction((IFunction) column)) {
                Object arg = ((IFunction) column).getArgs().get(0);
                if (arg instanceof QueryTreeNode) {
                    buildSubqueryOnFilter((IFunction) column);
                }
            }

            buildFunction((IFunction) column, findInSelectList);
            // 尝试下推function
            // pusherFunction((IFunction) column);
        }

        return column;
    }

    /**
     * 从select列表中查找
     */
    private ISelectable getColumnFromSelecteList(ISelectable c) {
        ISelectable column = null;
        for (ISelectable selected : this.getNode().getColumnsSelected()) {
            boolean isThis = false;

            if (c.getTableName() != null && (!(node instanceof KVIndexNode))) {
                if (!c.getTableName().equals(selected.getTableName())) {
                    continue;
                }
            }

            // 在当前select中查找，先比较column name，再比较alias name
            if (selected.getColumnName().equals(c.getColumnName())) {
                isThis = true;
            } else if (selected.getAlias() != null && selected.getAlias().equals(c.getColumnName())) {
                isThis = true;
            }

            if (isThis) {
                column = selected;
                return column;
            }
        }

        return column;
    }

    /**
     * 解决correlated subquery问题, 当前对象找不到，尝试找一下parent节点
     * 
     * <pre>
     * <a href="http://dev.mysql.com/doc/refman/5.6/en/correlated-subqueries.html">correlated subquery</a>
     * </pre>
     */
    protected ISelectable getSelectableFromParent(ISelectable c) {
        if (this.getNode().getParent() != null) {
            ISelectable column = this.getNode().getParent().getBuilder().getSelectableFromChild(c);
            if (column != null) {
                String alias = this.getNode().getParent().getAlias();
                if (alias != null) {
                    column.setTableName(alias);
                }
            }

            return column;
        } else {
            return null;
        }
    }

    protected abstract ISelectable getSelectableFromChild(ISelectable c);

    protected void pusherFunction(IFunction f) {

    }

    protected void buildWhere() {
        // sql语法中，where条件中的列不允许使用别名，所以无需从select中找列
        this.buildFilter(node.getKeyFilter(), false);
        this.buildFilter(node.getWhereFilter(), false);
        this.buildFilter(node.getResultFilter(), false);
        this.buildFilter(node.getOtherJoinOnFilter(), false);
        this.buildFilter(node.getSubqueryFilter(), false);
    }

    protected void buildFilter(IFilter filter, boolean findInSelectList) {
        if (filter == null) {
            return;
        }

        if (filter instanceof ILogicalFilter) {
            for (IFilter sub : ((ILogicalFilter) filter).getSubFilter()) {
                this.buildFilter(sub, findInSelectList);
            }
        } else if (filter instanceof IGroupFilter) {
            for (IFilter sub : ((IGroupFilter) filter).getSubFilter()) {
                this.buildFilter(sub, findInSelectList);
            }
        } else {
            buildBooleanFilter((IBooleanFilter) filter, findInSelectList);
        }
    }

    protected void buildBooleanFilter(IBooleanFilter filter, boolean findInSelectList) {
        if (filter == null) {
            return;
        }

        Object column = filter.getColumn();
        Object value = filter.getValue();

        // ================= 处理子查询=================
        // subQuery，比如WHERE ID = (SELECT ID FROM A)
        if (column instanceof IFunction && isSubqueryFunction((IFunction) column)) {
            Object arg = ((IFunction) column).getArgs().get(0);
            if (arg instanceof QueryTreeNode) {
                buildSubqueryOnFilter((IFunction) column);
                QueryTreeNode subquery = (QueryTreeNode) arg;
                subquery.build();
            }
        } else if (column instanceof ISelectable) {
            filter.setColumn(this.buildSelectable((ISelectable) column, findInSelectList));
        } else if (column instanceof ISequenceVal) {
            setExistSequenceVal();
        }

        // subQuery，比如WHERE ID = (SELECT ID FROM A)
        if (value instanceof IFunction && isSubqueryFunction((IFunction) value)) {
            Object arg = ((IFunction) value).getArgs().get(0);
            if (arg instanceof QueryTreeNode) {
                buildSubqueryOnFilter((IFunction) value);
                QueryTreeNode subquery = (QueryTreeNode) arg;
                subquery.build();
            }
        } else if (value instanceof ISelectable) {
            filter.setValue(this.buildSelectable((ISelectable) value, findInSelectList));
        } else if (value instanceof ISequenceVal) {
            setExistSequenceVal();
        }

        if (value != null && value instanceof IFunction && ((IFunction) value).getArgs().size() > 0) {
            // 特殊优化 ALL/ANY
            // val > ALL (SELECT...) -> val > MAX (SELECT...)
            // val < ALL (SELECT...) -> val < MIN (SELECT...)
            // val > ANY (SELECT...) -> val > MIN (SELECT...)
            // val < ANY (SELECT...) -> val < MAX (SELECT...)
            // val >= ALL (SELECT...) -> val >= MAX (SELECT...)
            // val <= ALL (SELECT...) -> val <= MIN (SELECT...)
            // val >= ANY (SELECT...) -> val >= MIN (SELECT...)
            // val <= ANY (SELECT...) -> val <= MAX (SELECT...)
            // http://dev.mysql.com/doc/internals/en/transformations-all-any.html
            Object arg = ((IFunction) value).getArgs().get(0);
            if (arg instanceof QueryTreeNode) {
                QueryTreeNode subquery = (QueryTreeNode) ((IFunction) value).getArgs().get(0);
                // 如果出现ALL，右边一定是subquery
                if (subquery.getColumnsSelected().size() != 1) { // 只能包含一列
                    throw new OptimizerException("Operand should contain 1 column(s)");
                }

                if (filter.getOperation().isAllOp()) {
                    // 针对出现group by时，不能简单替换为max/min值
                    if (!subquery.isExistAggregate()) {
                        ISelectable col = subquery.getColumnsSelected().get(0);
                        switch (filter.getOperation()) {
                            case GT_ALL:
                                // id > max(id)
                                subquery.select(buildMaxOrMinFunction(col, true));
                                filter.setOperation(OPERATION.GT);
                                break;
                            case GT_EQ_ALL:
                                // id > max(id)
                                subquery.select(buildMaxOrMinFunction(col, true));
                                filter.setOperation(OPERATION.GT_EQ);
                                break;
                            case LT_ALL:
                                // id < min(id)
                                subquery.select(buildMaxOrMinFunction(col, false));
                                filter.setOperation(OPERATION.LT);
                                break;
                            case LT_EQ_ALL:
                                // id < min(id)
                                subquery.select(buildMaxOrMinFunction(col, false));
                                filter.setOperation(OPERATION.LT_EQ);
                                break;
                            default:
                                break;
                        }
                    }

                    // 针对没有成功优化为max/min的，需要转化为list计算,让执行计划拿到list数据
                    if (filter.getOperation().isAllOp()) {
                        // 转变为list计算,让执行计划拿到list数据
                        ((IFunction) value).setFunctionName(IFunction.BuiltInFunction.SUBQUERY_LIST);
                    }
                } else if (filter.getOperation().isAnyOp()) {
                    // 针对出现group by时，不能简单替换为max/min值
                    if (!subquery.isExistAggregate()) {
                        ISelectable col = subquery.getColumnsSelected().get(0);
                        switch (filter.getOperation()) {
                            case GT_ANY:
                                // id > min(id)
                                subquery.select(buildMaxOrMinFunction(col, false));
                                filter.setOperation(OPERATION.GT);
                                break;
                            case GT_EQ_ANY:
                                // id > min(id)
                                subquery.select(buildMaxOrMinFunction(col, false));
                                filter.setOperation(OPERATION.GT_EQ);
                                break;
                            case LT_ANY:
                                // id < min(id)
                                subquery.select(buildMaxOrMinFunction(col, true));
                                filter.setOperation(OPERATION.LT);
                                break;
                            case LT_EQ_ANY:
                                // id < min(id)
                                subquery.select(buildMaxOrMinFunction(col, true));
                                filter.setOperation(OPERATION.LT_EQ);
                                break;
                            default:
                                break;
                        }
                    }

                    // 针对没有成功优化为max/min的，需要转化为list计算,让执行计划拿到list数据
                    if (filter.getOperation().isAnyOp()) {
                        // 转变为list计算,让执行计划拿到list数据
                        ((IFunction) value).setFunctionName(IFunction.BuiltInFunction.SUBQUERY_LIST);
                    }
                }
            }
        }

        if (filter.getOperation() == OPERATION.IN) {
            List<Object> values = filter.getValues();
            if (values != null && !values.isEmpty()) {
                // in的子查询
                if (values.get(0) instanceof IFunction && isSubqueryFunction((IFunction) values.get(0))) {
                    Object arg = ((IFunction) values.get(0)).getArgs().get(0);
                    if (arg instanceof QueryTreeNode) {
                        buildSubqueryOnFilter((IFunction) values.get(0));
                        QueryTreeNode subquery = (QueryTreeNode) arg;
                        subquery.build();
                    }
                } else {
                    for (Object val : values) {
                        if (val instanceof ISequenceVal) {
                            setExistSequenceVal();
                        }
                    }
                }
            }
        }
    }

    private boolean isSubqueryFunction(IFunction func) {
        return func.getFunctionName().equals(IFunction.BuiltInFunction.SUBQUERY_LIST)
               || func.getFunctionName().equals(IFunction.BuiltInFunction.SUBQUERY_SCALAR);
    }

    private IFunction buildMaxOrMinFunction(ISelectable column, boolean isMax) {
        IFunction func = ASTNodeFactory.getInstance().createFunction();
        func.setFunctionName(isMax ? "MAX" : "MIN");
        func.setColumnName(func.getFunctionName() + "(" + column.getColumnName() + ")");
        func.getArgs().add(column);
        return func;
    }

    protected void buildOrderBy() {
        for (IOrderBy order : node.getOrderBys()) {
            if (order.getColumn() instanceof ISelectable) {
                order.setColumn(this.buildSelectable(order.getColumn(), true));
            }
        }
    }

    protected void buildGroupBy() {
        for (IOrderBy order : node.getGroupBys()) {
            if (order.getColumn() instanceof ISelectable) {
                order.setColumn(this.buildSelectable(order.getColumn(), true));
            }
        }

        if (node.getGroupBys() != null && !node.getGroupBys().isEmpty()) {
            setExistAggregate();
        }
    }

    protected void buildHaving() {
        // having是允许使用select中的列的，如 havaing count(id)>1
        this.buildFilter(this.getNode().getHavingFilter(), true);
    }

    protected void buildFunction() {
        for (ISelectable selected : getNode().getColumnsSelected()) {
            if (selected instanceof IFunction) {
                this.buildFunction((IFunction) selected, false);
            }
        }
    }

    protected void buildFunction(IFunction f, boolean findInSelectList) {
        if (FunctionType.Aggregate == f.getFunctionType()) {
            setExistAggregate();
        }

        if (f.getArgs().size() == 0) {
            return;
        }

        List<Object> args = f.getArgs();
        for (int i = 0; i < args.size(); i++) {
            if (args.get(i) instanceof ISelectable) {
                args.set(i, this.buildSelectable((ISelectable) args.get(i), findInSelectList));
            } else if (args.get(i) instanceof ISequenceVal) {
                setExistSequenceVal();
            }
        }
    }

    public ISelectable findColumn(ISelectable c) {
        ISelectable column = this.findColumnFromOtherNode(c, this.getNode());
        if (column == null) {
            column = this.getSelectableFromChild(c);
        }

        return column;
    }

    protected void buildExistAggregate() {
        // 存在distinct
        for (ISelectable select : this.node.getColumnsRefered()) {
            if (select.isDistinct() || (select instanceof IFunction && ((IFunction) select).isNeedDistinctArg())) {
                setExistAggregate();
                return;
            }
        }

        // 如果子节点中有一个是聚合查询，则传递到父节点
        for (ASTNode sub : this.getNode().getChildren()) {
            if (sub instanceof QueryTreeNode) {
                if (((QueryTreeNode) sub).isExistAggregate()) {
                    setExistAggregate();
                    return;
                }
            }
        }
    }

    protected void buildExistSequenceVal() {
        // 如果子节点中有一个是聚合查询，则传递到父节点
        for (ASTNode sub : this.getNode().getChildren()) {
            if (sub instanceof QueryTreeNode) {
                if (((QueryTreeNode) sub).isExistSequenceVal()) {
                    setExistSequenceVal();
                    return;
                }
            } else if (sub instanceof DMLNode) {
                if (((DMLNode) sub).isExistSequenceVal()) {
                    setExistSequenceVal();
                    return;
                }
            }
        }
    }

    /**
     * 从select列表中查找字段，并根据查找的字段信息进行更新，比如更新tableName
     */
    protected ISelectable getColumnFromOtherNode(ISelectable c, QueryTreeNode other) {
        ISelectable res = findColumnFromOtherNode(c, other);
        if (res == null) {
            return null;
        }

        if (c instanceof IColumn) {
            c.setDataType(res.getDataType());
            c.setAutoIncrement(res.isAutoIncrement());
            // 如果是子表的结构，比如Join/Merge的子节点，字段的名字直接使用别名
            if (other.getAlias() != null) {
                c.setTableName(other.getAlias());
            } else {
                c.setTableName(res.getTableName());
            }
        }

        return c;
    }

    /**
     * 从select列表中查找字段
     */
    protected ISelectable findColumnFromOtherNode(ISelectable c, QueryTreeNode other) {
        if (c == null) {
            return c;
        }

        if (c instanceof IBooleanFilter && ((IBooleanFilter) c).getOperation().equals(OPERATION.CONSTANT)) {
            return c;
        }

        ISelectable res = null;
        for (ISelectable selected : other.getColumnsSelected()) {
            boolean isThis = false;
            if (c.getTableName() != null) {
                boolean isSameName = c.getTableName().equals(other.getAlias())
                                     || c.getTableName().equals(selected.getTableName());
                if (other.isSubQuery() && other.getSubAlias() != null) {
                    isSameName |= c.getTableName().equals(other.getSubAlias());
                }
                if (!isSameName) {
                    continue;
                }
            }

            if (IColumn.STAR.equals(c.getColumnName())) {
                return c;
            }

            // 若列别名存在，只比较别名
            isThis = c.isSameName(selected);

            if (isThis) {
                if (res != null) {
                    // 说明出现两个ID，需要明确指定TABLE
                    throw new IllegalArgumentException("Column: '" + c.getFullName() + "' is ambiguous by exist ["
                                                       + selected.getFullName() + "," + res.getFullName() + "]");
                }
                res = selected;
            }
        }

        return res;
    }

    protected void setExistAggregate() {
        this.node.setExistAggregate(true);
    }

    protected void setExistSequenceVal() {
        this.node.setExistSequenceVal(true);
    }

    protected void buildSubqueryOnFilter(IFunction func) {
        QueryTreeNode query = (QueryTreeNode) func.getArgs().get(0);
        Long subqueryOnFilterId = query.getSubqueryOnFilterId();
        if (subqueryOnFilterId.equals(0L)) {
            query.setSubqueryOnFilterId(UniqIdGen.genSubqueryID());
        }
        return;
    }

    /**
     * 目前orderby/groupby中的列必须在select列中存在
     */
    protected void checkOrderColumnExist(IOrderBy orderby, QueryTreeNode qtn) {
        ISelectable order = orderby.getColumn();
        if (order instanceof IFunction) {
            ISelectable column = getColumnFromOtherNode(order, qtn);
            if (column == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER_MISS_ORDER_FUNCTION_IN_SELECT,
                    "Column: " + orderby.getColumn() + " is not existed in select clause");
            }
        }
    }

}
