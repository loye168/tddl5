package com.taobao.tddl.optimizer.parse.cobar.visitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.misc.QueryExpression;
import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.RowExpression;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement.InsertMode;
import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public class MySqlInsertVisitor extends EmptySQLASTVisitor {

    private InsertNode insertNode;

    @Override
    public void visit(DMLInsertStatement node) {
        TableNode table = getTableNode(node);
        String insertColumns = this.getInsertColumnsStr(node);
        List<RowExpression> exprList = node.getRowList();

        if (exprList != null && exprList.size() == 1) {
            RowExpression expr = exprList.get(0);
            Object[] iv = getRowValue(expr);
            this.insertNode = table.insert(insertColumns, iv);
        } else if (exprList != null) {
            List<List<Object>> values = new ArrayList<List<Object>>();
            for (RowExpression expr : exprList) {
                Object[] iv = getRowValue(expr);
                values.add(Arrays.asList(iv));
            }

            this.insertNode = table.insert(insertColumns, values);
        } else {
            String[] columns = StringUtils.split(insertColumns, " ");
            List<ISelectable> cs = new ArrayList<ISelectable>();
            for (String name : columns) {
                ISelectable s = OptimizerUtils.createColumnFromString(name);
                cs.add(s);
            }
            this.insertNode = new InsertNode(table);
            this.insertNode.setColumns(cs);
        }

        if (node.isIgnore()) {
            insertNode.setIgnore(node.isIgnore());

        }
        if (InsertMode.DELAY == node.getMode()) {
            insertNode.setDelayed(true);
        }

        else if (InsertMode.HIGH == node.getMode()) {
            insertNode.setHighPriority(true);
        }

        else if (InsertMode.LOW == node.getMode()) {
            insertNode.setLowPriority(true);
        }

        List<Pair<Identifier, Expression>> cvs = node.getDuplicateUpdate();
        if (cvs != null && !cvs.isEmpty()) {
            Object[] updateValues = new Comparable[cvs.size()];
            String[] updateColumns = new String[cvs.size()];
            for (int i = 0; i < cvs.size(); i++) {
                Pair<Identifier, Expression> p = cvs.get(i);

                updateColumns[i] = (p.getKey().getIdTextUpUnescape());
                MySqlExprVisitor mv = new MySqlExprVisitor();
                p.getValue().accept(mv);
                updateValues[i] = mv.getColumnOrValue();// 可能为function
            }

            this.insertNode.duplicateUpdate(updateColumns, updateValues);
        }

        // 暂时不支持子表的查询
        QueryExpression subQuery = node.getSelect();
        if (subQuery != null) {
            MySqlSelectVisitor visitor = new MySqlSelectVisitor();
            subQuery.accept(visitor);
            insertNode.setSelectNode(visitor.getTableNode());
        }
    }

    private TableNode getTableNode(DMLInsertStatement node) {
        TableNode table = null;
        table = new TableNode(node.getTable().getIdTextUpUnescape());
        return table;
    }

    private String getInsertColumnsStr(DMLInsertStatement node) {
        List<Identifier> columnNames = node.getColumnNameList();
        StringBuilder sb = new StringBuilder("");
        if (columnNames != null && columnNames.size() != 0) {
            for (int i = 0; i < columnNames.size(); i++) {
                if (i > 0) {
                    sb.append(" ");
                }
                sb.append(columnNames.get(i).getIdTextUpUnescape());
            }
        }

        return sb.toString();
    }

    private Object[] getRowValue(RowExpression expr) {
        Object[] iv = new Object[expr.getRowExprList().size()];
        for (int i = 0; i < expr.getRowExprList().size(); i++) {
            MySqlExprVisitor mv = new MySqlExprVisitor();
            expr.getRowExprList().get(i).accept(mv);
            iv[i] = mv.getColumnOrValue();
        }

        return iv;
    }

    public InsertNode getInsertNode() {
        return insertNode;
    }
}
