package com.taobao.tddl.optimizer.parse.cobar.visitor;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.comparison.InExpression;
import com.alibaba.cobar.parser.ast.expression.misc.InExpressionList;
import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.ParamMarker;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralBitField;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralBoolean;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralHexadecimal;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralNull;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralNumber;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralString;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableRefFactor;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowColumns;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowCreate;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowCreate.Type;
import com.alibaba.cobar.parser.ast.stmt.ddl.DescTableStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLReplaceStatement;
import com.alibaba.cobar.parser.visitor.MySQLOutputASTVisitor;

/**
 * 格式化对应的sql
 * 
 * @author jianghang 2014-7-6 下午9:44:29
 * @since 5.1.7
 */
public class MysqlParameterizedSqlVistor extends MySQLOutputASTVisitor {

    private int                 replaceCount     = 0;
    private boolean             attrParamsSkip   = true;
    // schema名对应表名
    private Map<String, String> tablesWithSchema = new HashMap<String, String>();

    public MysqlParameterizedSqlVistor(StringBuilder appendable, boolean attrParamsSkip){
        super(appendable, null);
        this.attrParamsSkip = attrParamsSkip;
    }

    @Override
    public void visit(InExpression node) {
        InExpressionList inList = node.getInExpressionList();
        if (inList != null && !(inList.getList().size() == 1 && inList.getList().get(0) instanceof ParamMarker)) {
            appendable.append('(');
            if (node.isNot()) {
                appendable.append(" NOT IN (?)");
            } else {
                appendable.append(" IN (?)");
            }
            appendable.append(')');
            incrementReplaceCunt();
        } else {
            super.visit(node);
        }
    }

    @Override
    public void visit(LiteralBitField node) {
        if (!checkParameterize(node)) {
            super.visit(node);
        } else {
            appendable.append('?');
            incrementReplaceCunt();
        }
    }

    @Override
    public void visit(LiteralBoolean node) {
        if (!checkParameterize(node)) {
            super.visit(node);
        } else {
            appendable.append('?');
            incrementReplaceCunt();
        }
    }

    @Override
    public void visit(LiteralHexadecimal node) {
        if (!checkParameterize(node)) {
            super.visit(node);
        } else {
            appendable.append('?');
            incrementReplaceCunt();
        }
    }

    @Override
    public void visit(LiteralNull node) {
        if (!checkParameterize(node)) {
            super.visit(node);
        } else {
            appendable.append('?');
            incrementReplaceCunt();
        }
    }

    @Override
    public void visit(LiteralNumber node) {
        if (!checkParameterize(node)) {
            super.visit(node);
        } else {
            appendable.append('?');
            incrementReplaceCunt();
        }
    }

    @Override
    public void visit(LiteralString node) {
        if (!checkParameterize(node)) {
            super.visit(node);
        } else {
            appendable.append('?');
            incrementReplaceCunt();
        }
    }

    // ======================= table build ===================
    @Override
    public void visit(TableRefFactor node) {
        String schema = null;
        if (node.getTable().getParent() != null) {
            schema = node.getTable().getParent().getIdTextUpUnescape();
        }

        tablesWithSchema.put(node.getTable().getIdTextUpUnescape(), schema);
        super.visit(node);
    }

    @Override
    public void visit(ShowCreate node) {
        if (((ShowCreate) node).getType() == Type.TABLE) {
            String schema = null;
            if (((ShowCreate) node).getId().getParent() != null) {
                schema = ((ShowCreate) node).getId().getParent().getIdTextUpUnescape();
            }

            tablesWithSchema.put(((ShowCreate) node).getId().getIdTextUpUnescape(), schema);
        }
        super.visit(node);
    }

    @Override
    public void visit(DescTableStatement node) {
        String schema = null;
        if (node.getTable().getParent() != null) {
            schema = node.getTable().getParent().getIdTextUpUnescape();
        }

        tablesWithSchema.put(node.getTable().getIdTextUpUnescape(), schema);
        super.visit(node);
    }

    @Override
    public void visit(ShowColumns node) {
        String schema = null;
        if (((ShowColumns) node).getTable().getParent() != null) {
            schema = ((ShowColumns) node).getTable().getParent().getIdTextUpUnescape();
        }

        tablesWithSchema.put(((ShowColumns) node).getTable().getIdTextUpUnescape(), schema);
        super.visit(node);
    }

    @Override
    public void visit(DMLDeleteStatement node) {
        for (Identifier id : node.getTableNames()) {
            String schema = null;
            if (id.getParent() != null) {
                schema = id.getParent().getIdTextUpUnescape();
            }

            if (id.getIdTextUpUnescape().equals("*")) {
                tablesWithSchema.put(id.getParent().getIdTextUpUnescape(), schema);
            } else {
                tablesWithSchema.put(id.getIdTextUpUnescape(), schema);
            }
        }

        super.visit(node);
    }

    @Override
    public void visit(DMLInsertStatement node) {
        Identifier tb = node.getTable();
        String schema = null;
        if (tb.getParent() != null) {
            schema = tb.getParent().getIdTextUpUnescape();
        }

        tablesWithSchema.put(tb.getIdTextUpUnescape(), schema);
        super.visit(node);
    }

    @Override
    public void visit(DMLReplaceStatement node) {
        Identifier tb = node.getTable();
        String schema = null;
        if (tb.getParent() != null) {
            schema = tb.getParent().getIdTextUpUnescape();
        }
        tablesWithSchema.put(tb.getIdTextUpUnescape(), schema);
        super.visit(node);
    }

    public boolean checkParameterize(Expression e) {
        return attrParamsSkip;
    }

    public int getReplaceCount() {
        return replaceCount;
    }

    public void incrementReplaceCunt() {
        replaceCount++;
    }

    public Map<String, String> getTablesWithSchema() {
        return tablesWithSchema;
    }

}
