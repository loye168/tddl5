package com.taobao.tddl.optimizer.parse.cobar.visitor;

import com.alibaba.cobar.parser.ast.stmt.dml.DMLLoadStatement;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.dml.LoadDataNode;

public class MySqlLoadDataVisitor extends EmptySQLASTVisitor {

    private ASTNode node;

    @Override
    public void visit(DMLLoadStatement loadData) {

        String tableName = loadData.getTable().getIdTextUpUnescape();
        node = new LoadDataNode(tableName);
    }

    public ASTNode getNode() {
        return this.node;
    }
}
