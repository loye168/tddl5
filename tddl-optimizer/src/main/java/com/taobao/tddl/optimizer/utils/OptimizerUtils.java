package com.taobao.tddl.optimizer.utils;

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IGroupFilter;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.bean.BindVal;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.exception.OptimizerException;
import com.taobao.tddl.optimizer.parse.SqlAnalysisResult;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlExprVisitor;

/**
 * @since 5.0.0
 */
public class OptimizerUtils {

    public static Object convertType(Object value, DataType type) {
        if (value == null) {
            return null;
        }

        if (type == null || value instanceof BindVal || value instanceof IFunction || value instanceof NullValue) {
            return value;
        }

        if (DataTypeUtil.isDateType(type)) {
            // 针对时间类型，不做转换
            // 针对where date < '2014-05-23 00:00:01'，如果按照date类型进行转换，会丢失秒精度，导致结果出错
            return value;
        } else {
            return type.convertFrom(value);
        }
    }

    public static IFilter copyFilter(IFilter f) {
        return (IFilter) (f == null ? null : f.copy());
    }

    public static Set<ISelectable> copySelectables(Set<ISelectable> cs) {
        if (cs == null) {
            return null;
        }
        Set<ISelectable> news = new HashSet(cs.size());
        for (ISelectable c : cs) {
            news.add(c.copy());
        }

        return news;
    }

    public static List<ISelectable> copySelectables(List<ISelectable> cs) {
        if (cs == null) {
            return null;
        }

        List<ISelectable> news = new ArrayList(cs.size());
        for (ISelectable c : cs) {
            news.add(c.copy());
        }

        return news;
    }

    public static Object copyValue(Object obj) {
        if (obj instanceof IBindVal) {
            return ((IBindVal) obj).copy();
        } else if (obj instanceof ISelectable) {
            return ((ISelectable) obj).copy();
        } else if (obj instanceof IOrderBy) {
            return ((IOrderBy) obj).copy();
        } else {
            return obj;
        }
    }

    public static List<Object> copyValues(List<Object> objs) {
        if (objs == null) {
            return null;
        }

        List<Object> copys = new ArrayList<Object>(objs.size());
        for (Object obj : objs) {
            copys.add(OptimizerUtils.copyValue(obj));
        }

        return copys;
    }

    public static List copyFilter(List filters) {
        if (filters == null) {
            return null;
        }

        List newFilters = new ArrayList(filters.size());
        for (Object obj : filters) {
            IFilter f = (IFilter) obj;
            newFilters.add((IFilter) f.copy());
        }
        return newFilters;
    }

    public static List<IOrderBy> copyOrderBys(List<IOrderBy> orders) {
        if (orders == null) {
            return null;
        }

        List<IOrderBy> newOrders = new ArrayList<IOrderBy>(orders.size());

        for (IOrderBy o : orders) {
            newOrders.add(o.copy());
        }

        return newOrders;
    }

    public static List<ISelectable> copySelectables(List<ISelectable> selects, String oldTableName, String tableName) {
        if (tableName == null) {
            return copySelectables(selects);
        }

        if (selects == null) {
            return null;
        }

        List<ISelectable> news = new ArrayList(selects.size());
        for (ISelectable s : selects) {
            ISelectable a = s.copy();
            if (a instanceof IColumn) {
                setColumn((IColumn) a, oldTableName, tableName);
            } else if (a instanceof IFilter) {
                setFilter((IFilter) a, oldTableName, tableName);
            } else if (a instanceof IFunction) {
                setFunction((IFunction) a, oldTableName, tableName);
            }

            news.add(a);
        }

        return news;
    }

    public static List<IOrderBy> copyOrderBys(List<IOrderBy> orderBys, String oldTableName, String tableName) {
        if (tableName == null) {
            return copyOrderBys(orderBys);
        }

        if (orderBys == null) {
            return null;
        }

        List<IOrderBy> news = new ArrayList(orderBys.size());
        for (IOrderBy o : orderBys) {
            IOrderBy a = o.copy();
            if (a.getColumn() instanceof IColumn) {
                setColumn((IColumn) a.getColumn(), oldTableName, tableName);
            } else if (a.getColumn() instanceof IFilter) {
                setFilter((IFilter) a.getColumn(), oldTableName, tableName);
            } else if (a.getColumn() instanceof IFunction) {
                setFunction((IFunction) a.getColumn(), oldTableName, tableName);
            }

            news.add(a);
        }

        return news;
    }

    public static IFilter copyFilter(IFilter filter, String oldTableName, String tableName) {
        if (filter == null) {
            return null;
        }

        IFilter newFilter = (IFilter) filter.copy();
        if (tableName != null) {
            setFilter(newFilter, oldTableName, tableName);
        }
        return newFilter;
    }

    private static void setFunction(IFunction f, String oldTableName, String tableName) {
        for (Object arg : f.getArgs()) {
            if (arg instanceof ISelectable) {
                if (arg instanceof IColumn) {
                    setColumn((IColumn) arg, oldTableName, tableName);
                } else if (arg instanceof IFilter) {
                    setFilter((IFilter) arg, oldTableName, tableName);
                } else if (arg instanceof IFunction) {
                    setFunction((IFunction) arg, oldTableName, tableName);
                }
            }

        }
    }

    private static void setFilter(IFilter f, String oldTableName, String tableName) {
        if (f instanceof IBooleanFilter) {
            Object column = ((IBooleanFilter) f).getColumn();
            if (column instanceof IColumn) {
                setColumn((IColumn) column, oldTableName, tableName);
            } else if (column instanceof IFilter) {
                setFilter((IFilter) column, oldTableName, tableName);
            } else if (column instanceof IFunction) {
                setFunction((IFunction) column, oldTableName, tableName);
            }

            Object value = ((IBooleanFilter) f).getValue();
            if (value instanceof IColumn) {
                setColumn((IColumn) value, oldTableName, tableName);
            } else if (value instanceof IFilter) {
                setFilter((IFilter) value, oldTableName, tableName);
            } else if (value instanceof IFunction) {
                setFunction((IFunction) value, oldTableName, tableName);
            }
        } else if (f instanceof ILogicalFilter) {
            for (IFilter sf : ((ILogicalFilter) f).getSubFilter()) {
                setFilter(sf, oldTableName, tableName);
            }
        } else if (f instanceof IGroupFilter) {
            for (IFilter sf : ((IGroupFilter) f).getSubFilter()) {
                setFilter(sf, oldTableName, tableName);
            }
        }
    }

    private static void setColumn(IColumn c, String oldTableName, String tableName) {
        if (tableName != null && c.getTableName() != null
            && (oldTableName == null || StringUtils.equals(c.getTableName(), oldTableName))) {
            // 针对oldTableName为null时，也允许更新
            c.setTableName(tableName);
        }
    }

    /**
     * 根据索引信息，构建orderby条件
     */
    public static List<IOrderBy> getOrderBy(IndexMeta meta) {
        if (meta == null) {
            return new ArrayList<IOrderBy>(0);
        }

        List<IOrderBy> _orderBys = new ArrayList<IOrderBy>();
        for (ColumnMeta c : meta.getKeyColumns()) {
            IColumn column = ASTNodeFactory.getInstance().createColumn();
            column.setTableName(c.getTableName())
                .setColumnName(c.getName())
                .setDataType(c.getDataType())
                .setAlias(c.getAlias())
                .setAutoIncrement(c.isAutoIncrement());
            ;
            IOrderBy orderBy = ASTNodeFactory.getInstance().createOrderBy().setColumn(column).setDirection(true);
            _orderBys.add(orderBy);
        }
        return _orderBys;
    }

    /**
     * 根据column string构造{@linkplain ISelectable}对象
     * 
     * @param columnStr
     * @return
     */
    public static ISelectable createColumnFromString(String columnStr) {
        if (columnStr == null) {
            return null;
        }

        // 别名只能单独处理
        if (TStringUtil.containsIgnoreCase(columnStr, " AS ")) {
            String tmp[] = TStringUtil.splitByWholeSeparator(columnStr, " AS ");
            if (tmp.length != 2) {
                throw new RuntimeException("createColumnFromString:" + columnStr);
            }

            ISelectable c = createColumnFromString(tmp[0].trim());
            c.setAlias(tmp[1].trim());
            return c;
        } else {
            MySqlExprVisitor visitor = MySqlExprVisitor.parser(columnStr);
            Object value = MySqlExprVisitor.parser(columnStr).getColumnOrValue();
            if (value instanceof ISelectable) {
                return (ISelectable) value;
            } else if (value instanceof IFilter) {
                return (IFilter) value;
            } else { // 可能是常量
                return visitor.buildConstanctFilter(value);
            }
        }
    }

    public static IColumn columnMetaToIColumn(ColumnMeta m, String tableName) {
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setDataType(m.getDataType());
        c.setColumnName(m.getName());
        c.setTableName(tableName);
        c.setAlias(m.getAlias());
        c.setAutoIncrement(m.isAutoIncrement());
        return c;
    }

    public static IColumn columnMetaToIColumn(ColumnMeta m) {
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setDataType(m.getDataType());
        c.setColumnName(m.getName());
        c.setTableName(m.getTableName());
        c.setAlias(m.getAlias());
        c.setAutoIncrement(m.isAutoIncrement());
        return c;
    }

    public static IColumn getColumn(Object column) {
        if (column instanceof IFunction) {
            return ASTNodeFactory.getInstance()
                .createColumn()
                .setTableName(((IFunction) column).getTableName())
                .setColumnName(((IFunction) column).getColumnName())
                .setAlias(((IFunction) column).getAlias())
                .setDataType(((IFunction) column).getDataType());
        } else if (!(column instanceof IColumn)) {
            throw new IllegalArgumentException("column :" + column + " is not a icolumn");
        }

        return (IColumn) column;
    }

    /**
     * 将columnMeta转化为column列
     */
    public static List<ISelectable> columnMetaListToIColumnList(Collection<ColumnMeta> ms, String tableName) {
        List<ISelectable> cs = new ArrayList(ms.size());
        for (ColumnMeta m : ms) {
            cs.add(columnMetaToIColumn(m, tableName));
        }

        return cs;
    }

    public static List<ISelectable> columnMetaListToIColumnList(Collection<ColumnMeta> ms) {
        List<ISelectable> cs = new ArrayList(ms.size());
        for (ColumnMeta m : ms) {
            cs.add(columnMetaToIColumn(m));
        }

        return cs;
    }

    public static QueryTreeNode convertPlanToAst(IQueryTree plan) {
        if (plan == null) {
            return null;
        }

        if (plan instanceof IQuery) {
            return convertPlanToAst((IQuery) plan);
        } else if (plan instanceof IJoin) {
            return convertPlanToAst((IJoin) plan);
        } else if (plan instanceof IMerge) {
            return convertPlanToAst((IMerge) plan);
        } else {
            throw new OptimizerException("不支持的类型:" + plan);
        }
    }

    public static QueryTreeNode convertPlanToAst(IQuery query) {
        QueryTreeNode node = null;
        if (query.getSubQuery() == null) {
            node = new KVIndexNode(query.getIndexName());
        } else {
            node = new QueryNode();
            node.addChild(convertPlanToAst(query.getSubQuery()));
        }

        node.setAlias(query.getAlias());
        node.select(query.getColumns());
        node.setConsistent(query.getConsistent());
        node.setGroupBys(query.getGroupBys());
        node.setKeyFilter(query.getKeyFilter());
        node.setResultFilter(query.getValueFilter());
        node.setLimitFrom(query.getLimitFrom());
        node.setLimitTo(query.getLimitTo());
        node.setLockMode(query.getLockMode());
        node.setOrderBys(query.getOrderBys());
        node.setSql(query.getSql());
        node.having(query.getHavingFilter());
        node.setSubQuery(query.isSubQuery());
        node.setExistAggregate(query.isExistAggregate());
        node.setOtherJoinOnFilter(query.getOtherJoinOnFilter());
        node.setSubqueryFilter(query.getSubqueryFilter());
        node.executeOn(query.getDataNode());
        node.setSubqueryFilter(query.getSubqueryFilter());
        node.setSubqueryOnFilterId(query.getSubqueryOnFilterId());
        node.build();
        node = node.deepCopy();
        return node;
    }

    public static JoinNode convertPlanToAst(IJoin join) {
        JoinNode node = new JoinNode();
        node.setRightNode(convertPlanToAst(join.getRightNode()));
        node.setLeftNode(convertPlanToAst(join.getLeftNode()));
        node.setJoinStrategy(join.getJoinStrategy());
        if (join.getLeftOuter() && join.getRightOuter()) {
            node.setInnerJoin();
        } else if (join.getLeftOuter()) {
            node.setLeftOuterJoin();
        } else if (join.getRightOuter()) {
            node.setRightOuterJoin();
        } else {
            node.setOuterJoin();
        }

        int size = join.getLeftJoinOnColumns().size();
        for (int i = 0; i < size; i++) {
            node.addJoinKeys(join.getLeftJoinOnColumns().get(i), join.getRightJoinOnColumns().get(i));
        }
        node.setOrderBys(join.getOrderBys());
        node.setLimitFrom(join.getLimitFrom());
        node.setLimitTo(join.getLimitTo());
        node.setConsistent(join.getConsistent());
        node.setResultFilter(join.getValueFilter());
        node.having(join.getHavingFilter());
        node.setAlias(join.getAlias());
        node.setGroupBys(join.getGroupBys());
        node.setSubQuery(join.isSubQuery());
        node.setOtherJoinOnFilter(join.getOtherJoinOnFilter());
        node.select((join.getColumns()));
        node.setAllWhereFilter(join.getWhereFilter());
        node.setExistAggregate(join.isExistAggregate());
        node.executeOn(join.getDataNode());
        node.setSubqueryOnFilterId(join.getSubqueryOnFilterId());
        node.setSubqueryFilter(join.getSubqueryFilter());
        node.build();
        node = node.deepCopy();
        return node;
    }

    public static MergeNode convertPlanToAst(IMerge merge) {
        MergeNode node = new MergeNode();
        for (IDataNodeExecutor plan : merge.getSubNodes()) {
            if (!(plan instanceof IQueryTree)) {
                throw new OptimizerException("不支持将非merge(query)转为语法树");
            }
            node.addChild(convertPlanToAst((IQueryTree) plan));
        }
        node.setLimitFrom(merge.getLimitFrom());
        node.setLimitTo(merge.getLimitTo());
        node.select(merge.getColumns());
        node.setAlias(merge.getAlias());
        node.setSubQuery(merge.isSubQuery());
        node.setUnion(merge.isUnion());
        node.setOrderBys(merge.getOrderBys());
        node.setLimitFrom(merge.getLimitFrom());
        node.setLimitTo(merge.getLimitTo());
        node.setGroupBys(merge.getGroupBys());
        node.setSharded(merge.isSharded());
        node.having(merge.getHavingFilter());
        node.setOtherJoinOnFilter(merge.getOtherJoinOnFilter());
        node.setExistAggregate(merge.isExistAggregate());
        node.setGroupByShardColumns(merge.isGroupByShardColumns());
        node.setDistinctByShardColumns(merge.isDistinctByShardColumns());
        node.executeOn(merge.getDataNode());
        node.setSubqueryOnFilterId(merge.getSubqueryOnFilterId());
        node.setSubqueryFilter(merge.getSubqueryFilter());
        node.build();
        node = node.deepCopy();
        return node;
    }

    // --------------------------- assignment --------------------------

    public static IFilter assignment(IFilter f, Parameters parameterSettings) {
        if (f == null) {
            return null;
        }

        return (IFilter) f.assignment(parameterSettings);
    }

    public static ISelectable assignment(ISelectable c, Parameters parameterSettings) {
        if (c == null) {
            return c;
        }

        return c.assignment(parameterSettings);
    }

    public static List<ISelectable> assignment(List<ISelectable> cs, Parameters parameterSettings) {
        if (cs == null) {
            return cs;
        }
        for (ISelectable s : cs) {
            assignment(s, parameterSettings);
        }

        return cs;
    }

    /**
     * 整个执行计划是否无条件
     */
    public static boolean isNoFilter(IDataNodeExecutor dne) {
        if (!(dne instanceof IQueryTree)) {
            return true;
        }

        if (dne instanceof IMerge) {
            for (IDataNodeExecutor child : ((IMerge) dne).getSubNodes()) {
                return isNoFilter(child);
            }
        }

        if (dne instanceof IJoin) {
            return isNoFilter(((IJoin) dne).getLeftNode()) && isNoFilter(((IJoin) dne).getRightNode());
        }

        if (dne instanceof IQuery) {
            if (((IQuery) dne).getSubQuery() != null) {
                return isNoFilter(((IQuery) dne).getSubQuery());
            } else {
                return ((IQuery) dne).getKeyFilter() == null && ((IQuery) dne).getValueFilter() == null;
            }

        }

        return true;
    }

    public static Date parseDate(String str, String[] parsePatterns) throws ParseException {
        try {
            return parseDate(str, parsePatterns, Locale.ENGLISH);
        } catch (ParseException e) {
            return parseDate(str, parsePatterns, Locale.getDefault());
        }
    }

    public static Date parseDate(String str, String[] parsePatterns, Locale locale) throws ParseException {
        if ((str == null) || (parsePatterns == null)) {
            throw new IllegalArgumentException("Date and Patterns must not be null");
        }

        SimpleDateFormat parser = null;
        ParsePosition pos = new ParsePosition(0);

        for (int i = 0; i < parsePatterns.length; i++) {
            if (i == 0) {
                parser = new SimpleDateFormat(parsePatterns[0], locale);
            } else {
                parser.applyPattern(parsePatterns[i]);
            }
            pos.setIndex(0);
            Date date = parser.parse(str, pos);
            if ((date != null) && (pos.getIndex() == str.length())) {
                return date;
            }
        }

        throw new ParseException("Unable to parse the date: " + str, -1);
    }

    public static String parameterize(String sql) {
        return parameterize(sql, true);
    }

    public static String parameterize(String sql, boolean cache) {
        int i = 0;
        for (; i < sql.length(); ++i) {
            switch (sql.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
            }

            if (TStringUtil.startsWithIgnoreCase(sql, i, "explain")
                || TStringUtil.startsWithIgnoreCase(sql, i, "trace")) {
                return sql;
            }
        }

        SqlAnalysisResult result = OptimizerContext.getContext().getSqlParseManager().parse(sql, cache);
        return result.getParameterizedSql();
    }
}
