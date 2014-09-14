package com.taobao.tddl.repo.mysql.sqlconvertor;

import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.monitor.eagleeye.EagleeyeHelper;
import com.taobao.tddl.optimizer.core.CanVisit;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.dml.IDelete;
import com.taobao.tddl.optimizer.core.plan.dml.IInsert;
import com.taobao.tddl.optimizer.core.plan.dml.IReplace;
import com.taobao.tddl.optimizer.core.plan.dml.IUpdate;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.repo.mysql.function.FunctionStringConstructor;
import com.taobao.tddl.repo.mysql.function.FunctionStringConstructorManager;

public class MysqlPlanVisitorImpl implements PlanVisitor {

    protected boolean                          bindVal               = true;
    protected Map<Integer, ParameterContext>   outPutParamMap;
    protected Map<Integer, ParameterContext>   inPutParamMap;
    protected Map<Integer, Integer>            newParamIndexToOldMap = null;
    protected AtomicInteger                    bindValSequence;
    protected StringBuilder                    sqlBuilder            = new StringBuilder();
    protected FunctionStringConstructorManager manager               = new FunctionStringConstructorManager();
    protected IDataNodeExecutor                query;
    protected boolean                          isGroupBy             = false;

    private static Set<String>                 middleFuncName        = new HashSet<String>();

    static {
        middleFuncName.add("+");
        middleFuncName.add("-");
        middleFuncName.add("*");
        middleFuncName.add("/");
        middleFuncName.add("%");
        middleFuncName.add("&");
        middleFuncName.add("|");
        middleFuncName.add("=");
        middleFuncName.add("!=");
        middleFuncName.add("LIKE");
        middleFuncName.add("<>");
        middleFuncName.add(":=");
        middleFuncName.add("IS");
        middleFuncName.add("IN");
        middleFuncName.add("CONSTANT");
        middleFuncName.add("AND");
        middleFuncName.add("OR");
        middleFuncName.add("XOR");
        middleFuncName.add(">>");
        middleFuncName.add("<<");
        middleFuncName.add("DIV");
        middleFuncName.add("MOD");
        middleFuncName.add("&&");
        middleFuncName.add("||");
        middleFuncName.add(">");
        middleFuncName.add("<");
        middleFuncName.add(">=");
        middleFuncName.add("<=");
        middleFuncName.add("<>");
        middleFuncName.add("<=>");
        middleFuncName.add("^");
    }

    public MysqlPlanVisitorImpl(IDataNodeExecutor query, Map<Integer, ParameterContext> inputParamMap,
                                Map<Integer, ParameterContext> outputParamMap,
                                Map<Integer, Integer> newParamIndexToOldMap, AtomicInteger bindValSequence,
                                boolean bindVal){
        this(query, inputParamMap, outputParamMap, newParamIndexToOldMap, bindValSequence, bindVal, false);
    }

    public MysqlPlanVisitorImpl(IDataNodeExecutor query, Map<Integer, ParameterContext> inputParamMap,
                                Map<Integer, ParameterContext> outputParamMap,
                                Map<Integer, Integer> newParamIndexToOldMap, AtomicInteger bindValSequence,
                                boolean bindVal, boolean isGroupBy){
        this.query = query;
        this.inPutParamMap = inputParamMap;

        this.outPutParamMap = outputParamMap;

        if (this.outPutParamMap == null) {
            this.outPutParamMap = new HashMap<Integer, ParameterContext>();
        }

        this.newParamIndexToOldMap = newParamIndexToOldMap;

        if (this.newParamIndexToOldMap == null) {
            this.newParamIndexToOldMap = new HashMap();
        }

        if (bindValSequence != null) {
            this.bindValSequence = bindValSequence;
        } else {
            this.bindValSequence = new AtomicInteger(1);
        }

        this.bindVal = bindVal;
        this.isGroupBy = isGroupBy;
    }

    protected void buildGroupBy(IQueryTree<IQueryTree> query) {
        boolean first = true;
        if (query.getGroupBys() != null && !query.getGroupBys().isEmpty()) {
            sqlBuilder.append(" group by ");
            first = true;
            for (IOrderBy order : query.getGroupBys()) {
                if (first) {
                    first = false;
                } else {
                    sqlBuilder.append(",");
                }

                MysqlPlanVisitorImpl visitor = this.getOrderbyVisitor(order, true);
                sqlBuilder.append(visitor.getString());
            }
        }
    }

    protected void buildHaving(IQueryTree query) {
        if (query.getHavingFilter() != null) {
            sqlBuilder.append(" having ");
            MysqlPlanVisitorImpl visitor = this.getNewVisitor(query.getHavingFilter());
            sqlBuilder.append(visitor.getString());
        }
    }

    protected void buildLimit(IQueryTree query) {
        Long limitFrom = (Long) query.getLimitFrom();
        Long limitTo = (Long) query.getLimitTo();
        if ((limitFrom == null || limitFrom == -1) && (limitTo == null || limitTo == -1)) {
            return;
        }
        sqlBuilder.append(" limit ");
        MysqlPlanVisitorImpl visitor = this.getNewVisitor(limitFrom);
        sqlBuilder.append(visitor.getString());
        if (limitTo != null && limitTo != -1) {
            visitor = this.getNewVisitor(limitTo);
            sqlBuilder.append(",").append(visitor.getString());
        }
    }

    protected void buildOrderBy(IQueryTree<IQueryTree> query) {
        boolean first = true;
        if (query.getOrderBys() != null && !query.getOrderBys().isEmpty()) {
            sqlBuilder.append(" order by ");
            first = true;
            for (IOrderBy order : query.getOrderBys()) {
                if (first) {
                    first = false;
                } else {
                    sqlBuilder.append(",");
                }

                MysqlPlanVisitorImpl visitor = this.getOrderbyVisitor(order, false);
                sqlBuilder.append(visitor.getString());
            }
        }
    }

    public void buildSelect(IQueryTree<IQueryTree> query) {
        sqlBuilder.append("select ");
        boolean hasDistinct = false;
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (ISelectable selected : query.getColumns()) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            if (selected.isDistinct()) {
                hasDistinct = true;
            }
            MysqlPlanVisitorImpl visitor = this.getNewVisitor(selected);
            sb.append(visitor.getString());
            if (selected.getAlias() != null) {
                sb.append(" as ").append(selected.getAlias());
            }
        }

        if (hasDistinct) {
            sqlBuilder.append(" distinct ");
        }

        sqlBuilder.append(sb);
    }

    public MysqlPlanVisitorImpl getOrderbyVisitor(IOrderBy o, boolean isGroupBy) {
        MysqlPlanVisitorImpl visitor = new MysqlPlanVisitorImpl(query,
            inPutParamMap,
            outPutParamMap,
            newParamIndexToOldMap,
            bindValSequence,
            bindVal,
            isGroupBy);

        if (o instanceof CanVisit) {
            ((CanVisit) o).accept(visitor);
        } else {
            visitor.visit(o);
        }

        return visitor;
    }

    public MysqlPlanVisitorImpl getNewVisitor(Object o) {
        MysqlPlanVisitorImpl visitor = new MysqlPlanVisitorImpl(query,
            inPutParamMap,
            outPutParamMap,
            newParamIndexToOldMap,
            bindValSequence,
            bindVal,
            false);

        if (o instanceof CanVisit) {
            ((CanVisit) o).accept(visitor);
        } else {
            visitor.visit(o);
        }

        return visitor;
    }

    public MysqlPlanVisitorImpl getNewVisitor(IQueryTree query, Object o) {
        MysqlPlanVisitorImpl visitor = new MysqlPlanVisitorImpl(query,
            inPutParamMap,
            outPutParamMap,
            newParamIndexToOldMap,
            bindValSequence,
            bindVal,
            false);

        if (o instanceof CanVisit) {
            ((CanVisit) o).accept(visitor);
        } else {
            visitor.visit(o);
        }

        return visitor;
    }

    public Map<Integer, ParameterContext> getOutPutParamMap() {
        return outPutParamMap;
    }

    public Map<Integer, Integer> getNewParamIndexToOldMap() {
        return newParamIndexToOldMap;
    }

    public String getString() {
        return sqlBuilder.toString();
    }

    private boolean isMiddle(IFunction func) {
        if (middleFuncName.contains(func.getFunctionName())) {
            if (func.getArgs() != null && func.getArgs().size() == 1) {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    public void setParamMap(Map<Integer, ParameterContext> paramMap) {
        this.outPutParamMap = paramMap;
    }

    @Override
    public void visit(IColumn column) {
        // 别名加在select之外，如(select * from table) as t1,列名之前不能使用这个别名
        // 别名加在select之内，如select * from table as t1，列名之前可以使用这个别名
        if (query instanceof IQueryTree && !((IQueryTree) query).isSubQuery()
            && ((IQueryTree) query).getAlias() != null && column.getTableName() != null) {
            sqlBuilder.append(((IQueryTree) query).getAlias());
        } else {
            if (query instanceof IQuery && column.getTableName() != null) {
                sqlBuilder.append(((IQuery) query).getTableName());
            } else {
                if (query instanceof IQueryTree && column.getTableName() != null) {
                    sqlBuilder.append(column.getTableName());
                } else {
                    sqlBuilder.append(column.getColumnName());
                    return;
                }
            }
        }

        sqlBuilder.append(".");
        sqlBuilder.append(column.getColumnName());
    }

    @Override
    public void visit(IFilter filter) {
        visit((IFunction) filter);
    }

    @Override
    public void visit(IFunction func) {
        if (func.isNot()) {
            sqlBuilder.append("(");
            sqlBuilder.append(" NOT ");
        }
        String funcName = func.getFunctionName();
        FunctionStringConstructor constructor = manager.getConstructor(func);
        if (constructor != null) {
            sqlBuilder.append(constructor.constructColumnNameForFunction(query,
                bindVal,
                bindValSequence,
                outPutParamMap,
                func,
                this));
        } else {
            boolean isMiddle = isMiddle(func);
            if (isMiddle) {
                sqlBuilder.append("(");
                if (func instanceof IFilter) {
                    funcName = ((IFilter) func).getOperation().getOPERATIONString();
                }
            }
            if ((func instanceof IFilter) && OPERATION.CONSTANT.equals(((IFilter) func).getOperation())) {
                MysqlPlanVisitorImpl visitor = this.getNewVisitor(func.getArgs().get(0));
                sqlBuilder.append(visitor.getString());// 常量，不太可能走到这一步
            } else if ((func instanceof IBooleanFilter)
                       && (OPERATION.IS_NULL.equals(((IBooleanFilter) func).getOperation())
                           || OPERATION.IS_NOT_NULL.equals(((IBooleanFilter) func).getOperation())
                           || OPERATION.IS_TRUE.equals(((IBooleanFilter) func).getOperation())
                           || OPERATION.IS_NOT_TRUE.equals(((IBooleanFilter) func).getOperation())
                           || OPERATION.IS_FALSE.equals(((IBooleanFilter) func).getOperation()) || OPERATION.IS_NOT_FALSE.equals(((IBooleanFilter) func).getOperation())

                       )) {
                MysqlPlanVisitorImpl visitor = this.getNewVisitor(func.getArgs().get(0));
                sqlBuilder.append(visitor.getString());
                sqlBuilder.append(" ").append(funcName);
            } else {
                if (!isMiddle) {
                    if (IFunction.BuiltInFunction.MINUS.equals(funcName)) {
                        sqlBuilder.append("-");
                    } else if (!IFunction.BuiltInFunction.ROW.equals(funcName)) { // row代表向量匹配
                        sqlBuilder.append(funcName);
                    }

                    sqlBuilder.append("(");
                }
                boolean first = true;
                boolean isDistinct = false;
                StringBuilder argSb = new StringBuilder();
                for (Object arg : func.getArgs()) {
                    if (first) {
                        first = false;
                    } else if (isMiddle) {
                        argSb.append(" ").append(funcName).append(" ");
                    } else {
                        argSb.append(",");
                    }

                    if (arg instanceof ISelectable && ((ISelectable) arg).isDistinct()) {
                        isDistinct = true;
                    }
                    MysqlPlanVisitorImpl visitor = this.getNewVisitor(arg);
                    argSb.append(visitor.getString());
                }
                if (isDistinct) {
                    sqlBuilder.append(" distinct ");
                }
                sqlBuilder.append(argSb);
                if (!isMiddle) {
                    sqlBuilder.append(")");
                }
            }

            if (isMiddle) {
                sqlBuilder.append(")");
            }
        }
        if (func.isNot()) {
            sqlBuilder.append(")");
        }

    }

    @Override
    public void visit(IJoin join) {
        if (join.isSubQuery() && !join.isTopQuery()) {
            sqlBuilder.append(" ( ");
        }

        if (join.isSubQuery() || join.isTopQuery()) {
            buildSelect(join);
            sqlBuilder.append(" from ");
        }

        IQueryTree left = join.getLeftNode();
        IQueryTree right = join.getRightNode();

        MysqlPlanVisitorImpl visitor = this.getNewVisitor(left, left);
        sqlBuilder.append(visitor.getString());
        if (join.getLeftOuter() && join.getRightOuter()) {
            throw new RuntimeException("full outter join 不支持");
        } else if (join.getLeftOuter() && !join.getRightOuter()) {
            sqlBuilder.append(" left");
        } else if (join.getRightOuter() && !join.getLeftOuter()) {
            sqlBuilder.append(" right");
        }
        sqlBuilder.append(" join ");
        visitor = this.getNewVisitor(right, right);
        sqlBuilder.append(visitor.getString());
        sqlBuilder.append(" on ");
        StringBuilder joinOnFilterStr = new StringBuilder();
        boolean first = true;
        for (int i = 0; i < join.getLeftJoinOnColumns().size(); i++) {
            if (first) {
                first = false;
            } else {
                joinOnFilterStr.append(" and ");
            }
            ISelectable leftColumn = join.getLeftJoinOnColumns().get(i);
            ISelectable rightColumn = join.getRightJoinOnColumns().get(i);
            joinOnFilterStr.append(this.getNewVisitor(leftColumn).getString());
            joinOnFilterStr.append(" = ");
            joinOnFilterStr.append(this.getNewVisitor(rightColumn).getString());
        }

        if (join.getOtherJoinOnFilter() != null) {
            if (first) {
                first = false;
            } else {
                joinOnFilterStr.append(" and ");
            }

            joinOnFilterStr.append(this.getNewVisitor(join.getOtherJoinOnFilter()).getString());
        }

        sqlBuilder.append(joinOnFilterStr.toString());
        if (join.isSubQuery() || join.isTopQuery()) {
            String whereFilterStr = "";

            if (join.getWhereFilter() != null) {
                visitor = this.getNewVisitor(join.getWhereFilter());
                whereFilterStr = visitor.getString();
            }

            if (!TStringUtil.isEmpty(whereFilterStr)) {
                sqlBuilder.append(" where ");
                sqlBuilder.append(whereFilterStr);
            }
            buildGroupBy(join);
            buildHaving(join);
            buildOrderBy(join);
            buildLimit(join);

            switch (join.getLockMode()) {
                case EXCLUSIVE_LOCK:
                    sqlBuilder.append(" FOR UPDATE");
                    break;
                case SHARED_LOCK:
                    sqlBuilder.append(" LOCK IN SHARE MODE");
                    break;
                default:
                    break;
            }
        }

        if (join.isSubQuery() && !join.isTopQuery()) {
            sqlBuilder.append(" ) ");
            if (join.getAlias() != null) sqlBuilder.append(" ").append(join.getAlias()).append(" ");
        }
    }

    @Override
    public void visit(IOrderBy orderBy) {
        MysqlPlanVisitorImpl visitor = this.getNewVisitor(orderBy.getColumn());
        sqlBuilder.append(visitor.getString());

        if (!isGroupBy) {
            if (orderBy.getDirection()) {
                sqlBuilder.append(" asc ");
            } else {
                sqlBuilder.append(" desc ");
            }
        }
    }

    @Override
    public void visit(IQuery query) {
        if (query.isSubQuery() && !query.isTopQuery()) {
            sqlBuilder.append(" ( ");
        }
        if (query.isSubQuery() || query.isTopQuery()) {
            buildSelect(query);

            if (query.getTableName() == null && query.getSubQuery() == null) {
                return;
            }
            sqlBuilder.append(" from ");
        }

        if (query.getTableName() != null) {

            if (EagleeyeHelper.isTestMode()) {
                sqlBuilder.append(EagleeyeHelper.ALL_PERF_TABLE_PREFIX);
            }
            sqlBuilder.append(query.getTableName());

            if (!query.isSubQuery() && query.getAlias() != null
                && !query.getAlias().equalsIgnoreCase(query.getTableName())) {
                sqlBuilder.append(" ").append(query.getAlias());
            }
        } else if (query.getSubQuery() != null) {
            sqlBuilder.append(this.getNewVisitor(query.getSubQuery(), query.getSubQuery()).getString());
        }

        if (query.isSubQuery() || query.isTopQuery()) {
            String keyFilterStr = "";

            IFilter whereFilter = FilterUtils.and(FilterUtils.and(query.getKeyFilter(), query.getValueFilter()),
                query.getOtherJoinOnFilter());

            if (whereFilter != null) {
                MysqlPlanVisitorImpl visitor = this.getNewVisitor(whereFilter);
                keyFilterStr = visitor.getString();
            }

            if (!TStringUtil.isEmpty(keyFilterStr)) {
                sqlBuilder.append(" where ");
                sqlBuilder.append(keyFilterStr);
            }
            buildGroupBy(query);
            buildHaving(query);
            buildOrderBy(query);
            buildLimit(query);

            switch (query.getLockMode()) {
                case EXCLUSIVE_LOCK:
                    sqlBuilder.append(" FOR UPDATE");
                    break;
                case SHARED_LOCK:
                    sqlBuilder.append(" LOCK IN SHARE MODE");
                    break;
                default:
                    break;
            }
        }

        if (query.isSubQuery() && !query.isTopQuery()) {
            sqlBuilder.append(" ) ");
            if (query.getAlias() != null) {
                sqlBuilder.append(" ").append(query.getAlias()).append(" ");
            }
        }
    }

    @Override
    public void visit(List cl) {
        List<Comparable> list = cl;
        sqlBuilder.append("(");
        boolean first = true;
        for (Comparable o : list) {
            if (first) {
                first = false;
            } else {
                sqlBuilder.append(",");
            }
            MysqlPlanVisitorImpl visitor = this.getNewVisitor(o);
            sqlBuilder.append(visitor.getString());
        }
        sqlBuilder.append(")");
    }

    @Override
    public void visit(NullValue nullValue) {
        sqlBuilder.append("null");
        return;
    }

    @Override
    public void visit(Object s) {
        if (s instanceof List) {
            visit((List) s);
        } else {
            if (s instanceof Boolean) {
                sqlBuilder.append(((Boolean) s).toString());
                return;
            }

            if (s instanceof NullValue) {
                sqlBuilder.append("null");
                return;
            }

            int index = bindValSequence.getAndIncrement();
            ParameterContext context = null;
            if (s != null && !(s instanceof NullValue)) {
                context = new ParameterContext(ParameterMethod.setObject1, new Object[] { index, s });
            } else {
                context = new ParameterContext(ParameterMethod.setNull1, new Object[] { index, Types.NULL });
            }
            this.outPutParamMap.put(index, context);
            sqlBuilder.append("?");
        }

    }

    @Override
    public void visit(IBindVal bindVal) {
        int index = bindValSequence.getAndIncrement();
        ParameterContext context = null;

        context = new ParameterContext(inPutParamMap.get(bindVal.getOrignIndex()).getParameterMethod(), new Object[] {
                index, inPutParamMap.get(bindVal.getOrignIndex()).getArgs()[1] });

        this.newParamIndexToOldMap.put(index, bindVal.getOrignIndex());

        this.outPutParamMap.put(index, context);
        sqlBuilder.append("?");

    }

    @Override
    public void visit(IInsert put) {
        sqlBuilder.append("insert ");

        if (put.isLowPriority()) {
            sqlBuilder.append("low_priority ");
        } else if (put.isHighPriority()) {
            sqlBuilder.append("high_priority ");
        } else if (put.isDelayed()) {
            sqlBuilder.append("delayed ");
        }

        if (put.isIgnore()) {
            sqlBuilder.append("ignore into ");
        } else {
            sqlBuilder.append("into ");
        }

        if (EagleeyeHelper.isTestMode()) {
            sqlBuilder.append(EagleeyeHelper.ALL_PERF_TABLE_PREFIX);
        }
        sqlBuilder.append(put.getTableName()).append(" ");

        boolean first = true;
        if (!GeneralUtil.isEmpty(put.getUpdateColumns())) {
            sqlBuilder.append("( ");
            for (int i = 0; i < put.getUpdateColumns().size(); i++) {
                if (first) {
                    first = false;
                } else {
                    sqlBuilder.append(", ");
                }
                sqlBuilder.append(this.getNewVisitor(put.getUpdateColumns().get(i)).getString());
            }
            sqlBuilder.append(") ");
        }

        if (put.getQueryTree() != null) {
            ((IQueryTree) put.getQueryTree()).setTopQuery(true);
            MysqlPlanVisitorImpl visitor = this.getNewVisitor(put.getQueryTree(), put.getQueryTree());
            sqlBuilder.append(visitor.getString());
        } else {
            sqlBuilder.append("values ");
            for (int valuesIndex = 0; valuesIndex < put.getMultiValuesSize(); valuesIndex++) {
                if (valuesIndex == 0) {
                    sqlBuilder.append("( ");
                } else {
                    sqlBuilder.append(", ( ");
                }

                first = true;
                List<Object> values = put.getValues(valuesIndex);
                for (int i = 0; i < values.size(); i++) {
                    if (first) {
                        first = false;
                    } else {
                        sqlBuilder.append(", ");
                    }
                    sqlBuilder.append(this.getNewVisitor(values.get(i)).getString());
                }
                sqlBuilder.append(") ");
            }
        }

        if (put.getDuplicateUpdateColumns() != null && !put.getDuplicateUpdateColumns().isEmpty()) {
            sqlBuilder.append("on duplicate key update ");
            first = true;
            for (int i = 0; i < put.getDuplicateUpdateColumns().size(); i++) {
                if (first) {
                    first = false;
                } else {
                    sqlBuilder.append(", ");
                }

                sqlBuilder.append(this.getNewVisitor(put.getDuplicateUpdateColumns().get(i)).getString());
                sqlBuilder.append(" = ");
                sqlBuilder.append(this.getNewVisitor(put.getDuplicateUpdateValues().get(i)).getString());
            }

            sqlBuilder.append(" ");
        }
    }

    @Override
    public void visit(IReplace put) {
        sqlBuilder.append("replace ");

        if (put.isLowPriority()) {
            sqlBuilder.append("low_priority ");
        } else if (put.isIgnore()) {
            sqlBuilder.append("delayed ");
        }

        sqlBuilder.append("into ");
        if (EagleeyeHelper.isTestMode()) {
            sqlBuilder.append(EagleeyeHelper.ALL_PERF_TABLE_PREFIX);
        }
        sqlBuilder.append(put.getTableName()).append(" ");
        boolean first = true;
        if (!GeneralUtil.isEmpty(put.getUpdateColumns())) {
            sqlBuilder.append("( ");
            for (int i = 0; i < put.getUpdateColumns().size(); i++) {
                if (first) {
                    first = false;
                } else {
                    sqlBuilder.append(", ");
                }
                sqlBuilder.append(this.getNewVisitor(put.getUpdateColumns().get(i)).getString());
            }
            sqlBuilder.append(") ");
        }

        sqlBuilder.append("values ");
        for (int valuesIndex = 0; valuesIndex < put.getMultiValuesSize(); valuesIndex++) {

            if (valuesIndex == 0) {
                sqlBuilder.append("( ");
            } else {
                sqlBuilder.append(", ( ");
            }

            first = true;

            List<Object> values = put.getValues(valuesIndex);
            for (int i = 0; i < values.size(); i++) {
                if (first) {
                    first = false;
                } else {
                    sqlBuilder.append(", ");
                }
                sqlBuilder.append(this.getNewVisitor(values.get(i)).getString());
            }
            sqlBuilder.append(") ");
        }

    }

    @Override
    public void visit(IUpdate put) {
        sqlBuilder.append("update ");

        if (put.isLowPriority()) {
            sqlBuilder.append("low_priority ");
        }
        if (put.isIgnore()) {
            sqlBuilder.append("ignore ");
        }

        if (EagleeyeHelper.isTestMode()) {
            sqlBuilder.append(EagleeyeHelper.ALL_PERF_TABLE_PREFIX);
        }
        sqlBuilder.append(put.getTableName()).append(" ");

        sqlBuilder.append("set ");
        boolean first = true;

        if (put.getUpdateColumns() != null && !put.getUpdateValues().isEmpty()) {
            for (int i = 0; i < put.getUpdateColumns().size(); i++) {
                if (first) {
                    first = false;
                } else {
                    sqlBuilder.append(", ");
                }

                sqlBuilder.append(this.getNewVisitor(put.getUpdateColumns().get(i)).getString());
                sqlBuilder.append(" = ");
                sqlBuilder.append(this.getNewVisitor(put.getUpdateValues().get(i)).getString());
            }
        }
        String keyFilterStr = "";
        String resultFilterStr = "";

        IQueryTree query = put.getQueryTree();
        if (query instanceof IQuery && ((IQuery) query).getKeyFilter() != null) {
            MysqlPlanVisitorImpl visitor = this.getNewVisitor(((IQuery) query).getKeyFilter());
            keyFilterStr = visitor.getString();
        }

        if (query.getValueFilter() != null) {
            MysqlPlanVisitorImpl visitor = this.getNewVisitor(query.getValueFilter());
            resultFilterStr = visitor.getString();
        }

        if (!TStringUtil.isEmpty(keyFilterStr) || !TStringUtil.isEmpty(resultFilterStr)) {
            sqlBuilder.append(" where ");
            sqlBuilder.append(keyFilterStr);
            if (!TStringUtil.isEmpty(keyFilterStr)) {
                if (!TStringUtil.isEmpty(resultFilterStr)) {
                    sqlBuilder.append("and ");
                }

            }
            sqlBuilder.append(resultFilterStr);
            buildLimit(query);
        }

    }

    @Override
    public void visit(IDelete put) {
        sqlBuilder.append("delete ");

        if (put.isLowPriority()) {
            sqlBuilder.append("low_priority ");
        }

        if (put.isQuick()) {
            sqlBuilder.append("quick ");
        }

        if (put.isIgnore()) {
            sqlBuilder.append("ignore ");
        }
        sqlBuilder.append("from ");

        if (EagleeyeHelper.isTestMode()) {
            sqlBuilder.append(EagleeyeHelper.ALL_PERF_TABLE_PREFIX);
        }
        sqlBuilder.append(put.getTableName()).append(" ");

        String keyFilterStr = "";
        String resultFilterStr = "";

        IQueryTree query = put.getQueryTree();
        if (query instanceof IQuery && ((IQuery) query).getKeyFilter() != null) {
            MysqlPlanVisitorImpl visitor = this.getNewVisitor(((IQuery) query).getKeyFilter());
            keyFilterStr = visitor.getString();
        }

        if (query.getValueFilter() != null) {
            MysqlPlanVisitorImpl visitor = this.getNewVisitor(query.getValueFilter());
            resultFilterStr = visitor.getString();
        }

        if (!TStringUtil.isEmpty(keyFilterStr) || !TStringUtil.isEmpty(resultFilterStr)) {
            sqlBuilder.append(" where ");
            sqlBuilder.append(keyFilterStr);
            if (!TStringUtil.isEmpty(keyFilterStr)) {
                if (!TStringUtil.isEmpty(resultFilterStr)) {
                    sqlBuilder.append("and ");
                }

            }
            sqlBuilder.append(resultFilterStr);
        }

        buildLimit(query);
    }

}
