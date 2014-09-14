package com.taobao.tddl.optimizer.costbased;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ObjectUtils;

import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IGroupFilter;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exception.EmptyResultFilterException;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 预先处理filter条件
 * 
 * <pre>
 * 1. 判断永真/永假式,短化路径
 * 如： false or a = 1，优化为 a = 1
 * 如： true or a = 1，优化为true
 * 如： false or false，优化为{@linkplain EmptyResultFilterException}异常
 * 
 * 2. 调整下1 = id的列信息，优化为id = 1
 * 如：1 < a，调整为 a > 1 
 * 如：1 <= a，调整为 a >= 1
 * 如：1 > a，调整为 a < 1
 * 如：1 >= a，调整为 a <= 1
 * 其他情况，仅仅是交换下位置
 * 
 * 3. 根据column列，调整下value的类型
 * 如：a = 1，如果a是LONG型，会将文本1转化为Long. (sql解析后都会是纯文本信息)
 * </pre>
 */
public class FilterPreProcessor {

    /**
     * 处理逻辑见类描述 {@linkplain FilterPreProcessor}
     */
    public static QueryTreeNode optimize(QueryTreeNode qtn, boolean typeConvert, Map<String, Object> extraCmd) {
        qtn = preProcess(qtn, typeConvert, extraCmd);
        return qtn;
    }

    private static QueryTreeNode preProcess(QueryTreeNode qtn, boolean typeConvert, Map<String, Object> extraCmd) {
        qtn.setOtherJoinOnFilter(processFilter(qtn.getOtherJoinOnFilter(), typeConvert, extraCmd));
        qtn.having(processFilter(qtn.getHavingFilter(), typeConvert, extraCmd));
        qtn.query(processFilter(qtn.getWhereFilter(), typeConvert, extraCmd));
        qtn.setKeyFilter(processFilter(qtn.getKeyFilter(), typeConvert, extraCmd));
        qtn.setResultFilter(processFilter(qtn.getResultFilter(), typeConvert, extraCmd));
        if (qtn instanceof TableNode) {
            ((TableNode) qtn).setIndexQueryValueFilter(processFilter(((TableNode) qtn).getIndexQueryValueFilter(),
                typeConvert,
                extraCmd));
        }

        if (qtn instanceof JoinNode) {
            for (int i = 0; i < ((JoinNode) qtn).getJoinFilter().size(); i++) {
                processFilter(((JoinNode) qtn).getJoinFilter().get(i), typeConvert, extraCmd);
            }
        }

        for (ASTNode child : qtn.getChildren()) {
            preProcess((QueryTreeNode) child, typeConvert, extraCmd);
        }

        return qtn;
    }

    public static IFilter processFilter(IFilter root, boolean typeConvert, Map<String, Object> extraCmd) {
        if (root == null) {
            return null;
        }

        root = processOneFilter(root, typeConvert, extraCmd); // 做一下转换处理
        root = processGroupFilter(root, extraCmd); // 合并一下or group
        root = shortestFilter(root); // 短路一下
        root = FilterUtils.merge(root);// 合并一下flter
        return root;
    }

    /**
     * 将一个bool树中的节点合并为group，比如((A+B)+C)D转化为ED, E=(A+B+C)
     */
    private static IFilter processGroupFilter(IFilter node, Map<String, Object> extraCmd) {
        if (node == null) {
            return null;
        }

        if (isExpandOr(extraCmd)) {
            // 如果确认需要展开or，忽略group处理
            return node;
        }

        if (node.getOperation().equals(OPERATION.AND)) {
            ILogicalFilter logicNode = (ILogicalFilter) node;
            for (int i = 0; i < logicNode.getSubFilter().size(); i++) {
                logicNode.getSubFilter().set(i, processGroupFilter(logicNode.getSubFilter().get(i), extraCmd));
            }
        } else if (node.getOperation().equals(OPERATION.OR)) {
            ILogicalFilter logicNode = (ILogicalFilter) node;
            if (logicNode.getSubFilter().size() == 1) {
                return node;
            }

            ISelectable firstColumn = getSubGroupColumn(logicNode, 0, extraCmd);
            if (firstColumn == null) {
                return node;
            }

            // 判断可以进行合并
            IGroupFilter groupFilter = ASTNodeFactory.getInstance().createGroupFilter();
            groupFilter.setColumn(firstColumn.copy());
            groupFilter.getSubFilter().addAll(getSubGroupFilter(logicNode, 0));
            for (int i = 1; i < logicNode.getSubFilter().size(); i++) {
                ISelectable subColumn = getSubGroupColumn(logicNode, i, extraCmd);
                if (firstColumn != null && subColumn != null && subColumn.equals(firstColumn)) {
                    groupFilter.getSubFilter().addAll(getSubGroupFilter(logicNode, i));
                } else {
                    return node;
                }
            }

            // id = 3 or id = 4 转化为id in (3,4)
            if (isExpandIn(extraCmd)) {
                return groupFilter;
            }

            boolean canIn = true;
            List<Object> values = new ArrayList<Object>();
            for (IFilter f : groupFilter.getSubFilter()) {
                if (f.getOperation().equals(OPERATION.EQ)) {
                    values.add(((IBooleanFilter) f).getValue());
                } else if (f.getOperation().equals(OPERATION.IN)) {
                    values.addAll(((IBooleanFilter) f).getValues());
                } else {
                    canIn = false;
                    break;
                }
            }

            if (canIn) {
                // 优化为in
                IBooleanFilter inf = ASTNodeFactory.getInstance().createBooleanFilter();
                inf.setColumn(firstColumn);
                inf.setOperation(OPERATION.IN);
                inf.setValues(values);
                return inf;
            } else {
                return groupFilter;
            }
        }

        return node;
    }

    private static ISelectable getSubGroupColumn(ILogicalFilter parent, int index, Map<String, Object> extraCmd) {
        IFilter filter = parent.getSubFilter().get(index);
        if (filter.getOperation().equals(OPERATION.OR)) {
            filter = processGroupFilter(filter, extraCmd);
            parent.getSubFilter().set(index, filter);
            if (filter instanceof ILogicalFilter) {
                // 不符合传递性,直接退出
                return null;
            }
        }

        if (filter instanceof IBooleanFilter) {
            if (FilterUtils.isConstValue(((IBooleanFilter) filter).getColumn())) {
                // 列是常量
                return null;
            }

            if (!FilterUtils.isConstValue(((IBooleanFilter) filter).getValue())) {
                // value非常量
                return null;
            }

            return (ISelectable) ((IBooleanFilter) filter).getColumn();
        } else if (filter instanceof IGroupFilter) {
            return (ISelectable) ((IGroupFilter) filter).getColumn();
        }

        return null;
    }

    private static List<IFilter> getSubGroupFilter(ILogicalFilter parent, int index) {
        IFilter filter = parent.getSubFilter().get(index);
        List<IFilter> filters = new ArrayList<IFilter>();
        if (filter instanceof IGroupFilter) {
            for (IFilter subFilter : ((IGroupFilter) filter).getSubFilter()) {
                filters.add(subFilter);
            }
        } else {
            filters.add(filter);
        }

        return filters;
    }

    private static IFilter processOneFilter(IFilter root, boolean typeConvert, Map<String, Object> extraCmd) {
        if (root == null) {
            return null;
        }

        if (root instanceof IBooleanFilter) {
            return processBoolFilter(root, typeConvert, extraCmd);
        } else if (root instanceof IGroupFilter) {
            IGroupFilter lf = (IGroupFilter) root;
            List<IFilter> children = new LinkedList<IFilter>();
            for (IFilter child : lf.getSubFilter()) {
                IFilter childProcessed = processOneFilter(child, typeConvert, extraCmd);
                if (childProcessed != null) {
                    children.add(childProcessed);
                }
            }

            if (children.isEmpty()) {
                return null;
            }

            if (children.size() == 1) {
                return children.get(0);
            }

            lf.setSubFilter(children);
            return lf;
        } else if (root instanceof ILogicalFilter) {
            ILogicalFilter lf = (ILogicalFilter) root;
            List<IFilter> children = new LinkedList<IFilter>();
            for (IFilter child : lf.getSubFilter()) {
                IFilter childProcessed = processOneFilter(child, typeConvert, extraCmd);
                if (childProcessed != null) {
                    children.add(childProcessed);
                }
            }

            if (children.isEmpty()) {
                return null;
            }

            if (children.size() == 1) {
                return children.get(0);
            }

            lf.setSubFilter(children);
            return lf;
        }

        return root;
    }

    /**
     * 将0=1/1=1/true的恒等式进行优化
     */
    private static IFilter shortestFilter(IFilter root) throws EmptyResultFilterException {
        IFilter filter = FilterUtils.toDNFAndFlat(root);
        List<List<IFilter>> DNFfilter = FilterUtils.toDNFNodesArray(filter);

        List<List<IFilter>> newDNFfilter = new ArrayList<List<IFilter>>();
        for (List<IFilter> andDNFfilter : DNFfilter) {
            boolean isShortest = false;
            List<IFilter> newAndDNFfilter = new ArrayList<IFilter>();
            for (IFilter one : andDNFfilter) {
                if (one.getOperation() == OPERATION.CONSTANT) {
                    boolean flag = false;
                    if (((IBooleanFilter) one).getColumn() instanceof ISelectable) {// 可能是个not函数
                        newAndDNFfilter.add(one);// 不能丢弃
                    } else {
                        Object value = ((IBooleanFilter) one).getColumn();
                        if (value == null) {
                            flag = false;
                        } else if (value.getClass() == Boolean.class || value.getClass() == boolean.class) {
                            flag = (Boolean) value;
                        } else {
                            // mysql中字符串'true'会被当作0处理
                            flag = (DataType.LongType.convertFrom(value) != 0);
                        }

                        // if (StringUtils.isNumeric(value)) {
                        // flag =
                        // BooleanUtils.toBoolean(Integer.valueOf(value));
                        // } else {
                        // flag = BooleanUtils.toBoolean(((IBooleanFilter)
                        // one).getColumn().toString());
                        // }

                        if (!flag) {
                            isShortest = true;
                            break;
                        }
                    }
                } else {
                    newAndDNFfilter.add(one);
                }
            }

            if (!isShortest) {
                if (newAndDNFfilter.isEmpty()) {
                    // 代表出现为true or xxx，直接返回true
                    IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
                    f.setOperation(OPERATION.CONSTANT);
                    f.setColumn("1");
                    f.setColumnName(ObjectUtils.toString("1"));
                    return f;
                } else {// 针对非false的情况
                    newDNFfilter.add(newAndDNFfilter);
                }
            }
        }

        if (newDNFfilter.isEmpty()) {
            throw new EmptyResultFilterException();
        }

        return FilterUtils.DNFToOrLogicTree(newDNFfilter);
    }

    private static IFilter processBoolFilter(IFilter root, boolean typeConvert, Map<String, Object> extraCmd) {
        root = exchage(root);

        if (typeConvert) {
            root = typeConvert(root);
        }

        root = expandIn(root, extraCmd);
        return root;
    }

    /**
     * 如果是1 = id的情况，转化为id = 1
     */
    private static IFilter exchage(IFilter root) {
        IBooleanFilter bf = (IBooleanFilter) root;
        if (!FilterUtils.isConstValue(bf.getValue()) && FilterUtils.isConstValue(bf.getColumn())) {
            Object val = bf.getColumn();
            bf.setColumn(bf.getValue());
            bf.setValue(val);
            OPERATION newOp = bf.getOperation();
            switch (bf.getOperation()) {
                case GT:
                    newOp = OPERATION.LT;
                    break;
                case LT:
                    newOp = OPERATION.GT;
                    break;
                case GT_EQ:
                    newOp = OPERATION.LT_EQ;
                    break;
                case LT_EQ:
                    newOp = OPERATION.GT_EQ;
                    break;
                default:
                    break;
            }
            bf.setOperation(newOp);
        }
        return bf;
    }

    private static IFilter typeConvert(IFilter root) {
        IBooleanFilter bf = (IBooleanFilter) root;
        // 如果是id in (xx)
        if (bf.getValues() != null) {
            if (bf.getColumn() instanceof IColumn) {
                List<Object> values = new ArrayList<Object>();
                for (int i = 0; i < bf.getValues().size(); i++) {
                    values.add(OptimizerUtils.convertType(bf.getValues().get(i),
                        ((IColumn) bf.getColumn()).getDataType()));
                }
                bf.setValues(values);
            }
        } else {
            // 如果是 1 = id情况
            if (FilterUtils.isConstValue(bf.getColumn()) && !FilterUtils.isConstValue(bf.getValue())) {
                DataType type = null;
                if (bf.getValue() instanceof IColumn) {
                    type = ((IColumn) bf.getValue()).getDataType();
                }

                // if (bf.getValue() instanceof IFunction) {
                // type = ((IFunction) bf.getValue()).getDataType();
                // }

                bf.setColumn(OptimizerUtils.convertType(bf.getColumn(), type));
            }

            // 如果是 id = 1情况
            if (FilterUtils.isConstValue(bf.getValue()) && !FilterUtils.isConstValue(bf.getColumn())) {
                DataType type = null;
                if (bf.getColumn() instanceof IColumn) {
                    type = ((IColumn) bf.getColumn()).getDataType();
                }

                // if (bf.getColumn() instanceof IFunction) {
                // type = ((IFunction) bf.getColumn()).getDataType();
                // }

                bf.setValue(OptimizerUtils.convertType(bf.getValue(), type));
            }
        }
        return bf;
    }

    private static IFilter expandIn(IFilter root, Map<String, Object> extraCmd) {
        IBooleanFilter bf = (IBooleanFilter) root;
        if (bf.getOperation() == OPERATION.IN && isExpandIn(extraCmd)) {
            List<Object> values = bf.getValues();
            IFilter newRoot = null;
            for (Object value : values) {
                newRoot = FilterUtils.or(newRoot, FilterUtils.equal(bf.getColumn(), value));
            }

            return newRoot;
        } else {
            return root;
        }
    }

    private static boolean isExpandOr(Map<String, Object> extraCmd) {
        return GeneralUtil.getExtraCmdBoolean(extraCmd, ConnectionProperties.EXPAND_OR, false);
    }

    private static boolean isExpandIn(Map<String, Object> extraCmd) {
        return GeneralUtil.getExtraCmdBoolean(extraCmd, ConnectionProperties.EXPAND_IN, false);
    }
}
