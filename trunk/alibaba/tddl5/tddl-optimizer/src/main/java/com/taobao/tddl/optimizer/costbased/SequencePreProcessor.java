package com.taobao.tddl.optimizer.costbased;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IGroupFilter;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISequenceVal;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.optimizer.exception.OptimizerException;
import com.taobao.tddl.optimizer.sequence.ISequenceManager;

/**
 * sqeuence的处理类
 * 
 * <pre>
 * 1. 遍历下所有的节点，设置sequence.nextval的下标
 * 
 * <pre>
 * @author jianghang 2014-5-5 上午11:38:30
 * @since 5.1.0
 */
public class SequencePreProcessor {

    public static ASTNode opitmize(ASTNode node, Parameters parameters, Map<String, Object> extraCmd)
                                                                                                     throws OptimizerException {
        if (node instanceof DMLNode) {
            findDML((DMLNode) node, parameters);
        } else {
            findQuery((QueryTreeNode) node, parameters);
        }
        return node;
    }

    private static void findDML(DMLNode node, Parameters parameters) {
        List<ISelectable> columns = node.getColumns();
        if (node.isMultiValues()) {
            if (node.getMultiValues() != null) {
                for (Object objs : node.getMultiValues()) {
                    for (int i = 0; i < ((List) objs).size(); i++) {
                        Object obj = ((List) objs).get(i);
                        if (node.processAutoIncrement()) {
                            obj = convertToSequence(columns.get(i), obj, parameters);
                            ((List) objs).set(i, obj);
                        }

                        findObject(obj, parameters);
                    }
                }
            }
        } else {
            if (node.getValues() != null) {
                for (int i = 0; i < node.getValues().size(); i++) {
                    Object obj = node.getValues().get(i);
                    if (node.processAutoIncrement()) {
                        obj = convertToSequence(columns.get(i), obj, parameters);
                        node.getValues().set(i, obj);
                    }

                    findObject(obj, parameters);
                }
            }
        }

        if (node instanceof InsertNode) {
            InsertNode insert = (InsertNode) node;
            if (insert.getDuplicateUpdateValues() != null) {
                for (Object obj : insert.getDuplicateUpdateValues()) {
                    findObject(obj, parameters);
                }
            }
        } else if (node instanceof UpdateNode) {
            UpdateNode update = (UpdateNode) node;
            if (update.getUpdateValues() != null) {
                for (Object obj : update.getUpdateValues()) {
                    findObject(obj, parameters);
                }
            }
        }
    }

    public static Object convertToSequence(ISelectable column, Object value, Parameters parameterSettings) {
        if (column.isAutoIncrement() && !(value instanceof ISequenceVal)) {
            // 处理自增
            if (value == null || value instanceof NullValue) {
                // 识别null值
                return buildSequenceVal(column);
            } else if (value instanceof IBindVal) {
                Object cvalue = ((IBindVal) value).copy();
                // 识别绑定变量
                if (parameterSettings != null && parameterSettings.isBatch()) {
                    // boolean isAllNull = true;
                    // boolean existNull = false;
                    // for (int i = 0; i < parameterSettings.getBatchSize();
                    // i++) {
                    // Object bvalue = ((IBindVal)
                    // value).assignment(parameterSettings.cloneByBatchIndex(i));
                    // if (bvalue instanceof IBindVal) {
                    // bvalue = ((IBindVal) bvalue).getValue();
                    // }
                    //
                    // existNull |= (bvalue == null || bvalue instanceof
                    // NullValue);
                    // isAllNull &= (bvalue == null || bvalue instanceof
                    // NullValue);
                    // }
                    //
                    // if (existNull && !isAllNull) {// 部分出现null
                    // throw new
                    // OptimizerException("auto_increment only support all null values");
                    // } else if (isAllNull) {
                    // return buildSequenceVal(column);
                    // }

                    Object bvalue = ((IBindVal) cvalue).assignment(parameterSettings.cloneByBatchIndex(0));
                    // 可能batch模式，还是返回bindVal对象
                    if (bvalue instanceof IBindVal) {
                        bvalue = ((IBindVal) bvalue).getValue();
                    }

                    if (bvalue == null || bvalue instanceof NullValue) {
                        return buildSequenceVal(column);
                    }
                } else {
                    Object bvalue = ((IBindVal) cvalue).assignment(parameterSettings);
                    // 可能batch模式，还是返回bindVal对象
                    if (bvalue instanceof IBindVal) {
                        bvalue = ((IBindVal) bvalue).getValue();
                    }

                    if (bvalue == null || bvalue instanceof NullValue) {
                        return buildSequenceVal(column);
                    }
                }
            }
        }

        return value;
    }

    public static ISequenceVal buildSequenceVal(ISelectable column) {
        return ASTNodeFactory.getInstance().createSequenceValue(ISequenceManager.AUTO_SEQ_PREFIX
                                                                + column.getTableName());
    }

    private static void findQuery(QueryTreeNode qtn, Parameters parameters) {
        findFilter(qtn.getKeyFilter(), parameters);
        findFilter(qtn.getWhereFilter(), parameters);
        findFilter(qtn.getResultFilter(), parameters);
        findFilter(qtn.getOtherJoinOnFilter(), parameters);
        findFilter(qtn.getHavingFilter(), parameters);

        for (ISelectable select : qtn.getColumnsSelected()) {
            // 可能替换了subquery
            findSelectable(select, parameters);
        }

        if (qtn.getOrderBys() != null) {
            for (IOrderBy orderBy : qtn.getOrderBys()) {
                findOrderBy(orderBy, parameters);
            }
        }

        if (qtn.getGroupBys() != null) {
            for (IOrderBy groupBy : qtn.getGroupBys()) {
                findOrderBy(groupBy, parameters);
            }
        }
    }

    private static void findFilter(IFilter filter, Parameters parameters) {
        if (filter == null) {
            return;
        }

        if (filter instanceof ILogicalFilter) {
            for (IFilter sub : ((ILogicalFilter) filter).getSubFilter()) {
                findFilter(sub, parameters);
            }
        } else if (filter instanceof IGroupFilter) {
            for (IFilter sub : ((IGroupFilter) filter).getSubFilter()) {
                findFilter(sub, parameters);
            }
        } else {
            findBooleanFilter((IBooleanFilter) filter, parameters);
        }

    }

    private static void findBooleanFilter(IBooleanFilter filter, Parameters parameters) {
        if (filter == null) {
            return;
        }

        findObject(filter.getColumn(), parameters);
        findObject(filter.getValue(), parameters);
        if (filter.getOperation() == OPERATION.IN) {
            List<Object> values = filter.getValues();
            if (values != null && !values.isEmpty()) {
                for (int i = 0; i < values.size(); i++) {
                    findObject(values.get(i), parameters);
                }
            }
        }

    }

    private static void findObject(Object obj, Parameters parameters) {
        if (obj instanceof ISelectable) {
            findSelectable((ISelectable) obj, parameters);
        } else if (obj instanceof ISequenceVal) {
            findSequenceVal((ISequenceVal) obj, parameters);
        } else if (obj instanceof QueryTreeNode) { // scalar subquery
            // 深度优先,尝试递归找一下
            findQuery((QueryTreeNode) obj, parameters);
        }
    }

    private static void findSelectable(ISelectable select, Parameters parameters) {
        if (select instanceof IFilter) {
            findFilter((IFilter) select, parameters);
        } else if (select instanceof IFunction) {
            findFunction((IFunction) select, parameters);
        } else if (select instanceof IColumn) {
            findColumn((IColumn) select, parameters);
        }
    }

    private static void findColumn(IColumn column, Parameters parameters) {
        // do nothing
    }

    private static void findOrderBy(IOrderBy order, Parameters parameters) {
        if (order.getColumn() instanceof ISelectable) {
            findSelectable(order.getColumn(), parameters);
        }
    }

    private static void findFunction(IFunction f, Parameters parameters) {
        for (Object arg : f.getArgs()) {
            findObject(arg, parameters);
        }
    }

    private static void findSequenceVal(ISequenceVal s, Parameters parameters) {
        int index = parameters.getFirstParameter().size() + parameters.getSequenceSize().incrementAndGet();
        ((ISequenceVal) s).setOriginIndex(index);
    }

}
