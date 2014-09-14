package com.taobao.tddl.optimizer.parse.cobar.visitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.RowExpression;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLReplaceStatement;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;

/**
 * replace处理
 * 
 * @since 5.0.0
 */
public class MySqlReplaceVisitor extends EmptySQLASTVisitor {

    private PutNode replaceNode;

    @Override
    public void visit(DMLReplaceStatement node) {
        TableNode table = getTableNode(node);
        String insertColumns = this.getInsertColumnsStr(node);
        List<RowExpression> exprList = node.getRowList();
        if (exprList != null && exprList.size() == 1) {
            RowExpression expr = exprList.get(0);
            Object[] iv = getRowValue(expr);
            this.replaceNode = table.put(insertColumns, iv);
        } else {
            List<List<Object>> values = new ArrayList<List<Object>>();
            for (RowExpression expr : exprList) {
                Object[] iv = getRowValue(expr);
                values.add(Arrays.asList(iv));
            }

            this.replaceNode = table.put(insertColumns, values);
        }

        switch (node.getMode()) {
            case DELAY:
                this.replaceNode.setDelayed(true);
                break;
            case LOW:
                this.replaceNode.setLowPriority(true);
                break;
            case UNDEF:
                break;
            default:
                throw new IllegalArgumentException("unknown mode for INSERT: " + node.getMode());
        }
    }

    private TableNode getTableNode(DMLReplaceStatement node) {
        return new TableNode(node.getTable().getIdTextUpUnescape());
    }

    private String getInsertColumnsStr(DMLReplaceStatement node) {
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

    public PutNode getReplaceNode() {
        return replaceNode;
    }
}
