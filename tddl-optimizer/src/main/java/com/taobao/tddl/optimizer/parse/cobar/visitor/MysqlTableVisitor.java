package com.taobao.tddl.optimizer.parse.cobar.visitor;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableRefFactor;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowColumns;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowCreate;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowCreate.Type;
import com.alibaba.cobar.parser.ast.stmt.ddl.DescTableStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLReplaceStatement;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;

/**
 * 提取sql中的表名
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2012-10-26上午09:56:35
 */
public class MysqlTableVisitor extends EmptySQLASTVisitor {

    // schema名对应表名
    private Map<String, String> tablesWithSchema = new HashMap<String, String>();

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

    public Map<String, String> getTablesWithSchema() {
        return tablesWithSchema;
    }
}
