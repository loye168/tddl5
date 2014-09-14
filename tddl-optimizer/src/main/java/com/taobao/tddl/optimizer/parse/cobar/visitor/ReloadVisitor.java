package com.taobao.tddl.optimizer.parse.cobar.visitor;

import com.alibaba.cobar.parser.ast.stmt.reload.ReloadSchema;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.reload.ReloadNode;
import com.taobao.tddl.optimizer.core.ast.reload.ReloadNode.ReloadType;

public class ReloadVisitor extends EmptySQLASTVisitor {

    private ASTNode node;

    @Override
    public void visit(ReloadSchema reload) {

        ReloadNode reloadNode = new ReloadNode();
        reloadNode.setType(ReloadType.SCHEMA);

        this.node = reloadNode;
    }

    public ASTNode getNode() {
        return this.node;
    }
}
