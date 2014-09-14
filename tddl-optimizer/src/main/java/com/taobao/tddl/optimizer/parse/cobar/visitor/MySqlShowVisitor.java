package com.taobao.tddl.optimizer.parse.cobar.visitor;

import com.alibaba.cobar.parser.ast.stmt.dal.ShowBroadcasts;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowColumns;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowCreate;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowDatasources;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowIndex;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowPartitions;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowRule;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowTables;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowTopology;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowTrace;
import com.alibaba.cobar.parser.ast.stmt.ddl.DescTableStatement;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.dal.BaseShowNode;
import com.taobao.tddl.optimizer.core.ast.dal.BaseShowNode.ShowType;
import com.taobao.tddl.optimizer.core.ast.dal.ShowWithTableNode;
import com.taobao.tddl.optimizer.core.ast.dal.ShowWithoutTableNode;

public class MySqlShowVisitor extends EmptySQLASTVisitor {

    private ASTNode node;

    @Override
    public void visit(ShowTopology showTopology) {
        String name = showTopology.getName().getIdTextUpUnescape();
        ShowWithTableNode node = new ShowWithTableNode(name);
        node.setType(ShowType.TOPOLOGY);
        this.node = node;
    }

    @Override
    public void visit(ShowPartitions showPartitions) {
        String name = showPartitions.getName().getIdTextUpUnescape();
        ShowWithTableNode node = new ShowWithTableNode(name);
        node.setType(ShowType.PARTITIONS);
        this.node = node;
    }

    @Override
    public void visit(ShowTables showTables) {
        ShowWithoutTableNode node = new ShowWithoutTableNode();
        node.setFull(showTables.isFull());
        node.setPattern(showTables.getPattern());
        if (showTables.getWhere() != null) {
            MySqlExprVisitor visitor = new MySqlExprVisitor();
            showTables.getWhere().accept(visitor);
            node.setWhereFilter(visitor.getFilter());
        }
        node.setType(ShowType.TABLES);
        this.node = node;
    }

    @Override
    public void visit(ShowBroadcasts showBroadcasts) {
        ShowWithoutTableNode node = new ShowWithoutTableNode();
        node.setType(ShowType.BRAODCASTS);
        this.node = node;
    }

    @Override
    public void visit(ShowTrace showTrace) {
        ShowWithoutTableNode node = new ShowWithoutTableNode();
        node.setType(ShowType.TRACE);
        this.node = node;
    }

    @Override
    public void visit(ShowDatasources showDs) {
        ShowWithoutTableNode node = new ShowWithoutTableNode();
        node.setType(ShowType.DATASOURCES);
        this.node = node;
    }

    @Override
    public void visit(ShowRule showRule) {
        BaseShowNode node = null;
        if (showRule.getName() != null) {
            node = new ShowWithTableNode(showRule.getName().getIdTextUpUnescape());
        } else {
            node = new ShowWithoutTableNode();
        }
        node.setType(ShowType.RULE);
        this.node = node;
    }

    @Override
    public void visit(ShowCreate showCreate) {
        ShowWithTableNode node = new ShowWithTableNode(showCreate.getId().getIdTextUpUnescape());
        node.setType(ShowType.CREATE_TABLE);
        this.node = node;
    }

    @Override
    public void visit(DescTableStatement descTable) {
        ShowWithTableNode node = new ShowWithTableNode(descTable.getTable().getIdTextUpUnescape());
        node.setType(ShowType.DESC);
        this.node = node;
    }

    @Override
    public void visit(ShowColumns showColumns) {
        ShowWithTableNode node = new ShowWithTableNode(showColumns.getTable().getIdTextUpUnescape());
        if (showColumns.getWhere() != null) {
            MySqlExprVisitor visitor = new MySqlExprVisitor();
            showColumns.getWhere().accept(visitor);
            node.setWhereFilter(visitor.getFilter());
        }
        node.setFull(showColumns.isFull());
        node.setPattern(showColumns.getPattern());
        node.setType(ShowType.COLUMNS);
        this.node = node;
    }

    @Override
    public void visit(ShowIndex showIndex) {
        ShowWithTableNode node = new ShowWithTableNode(showIndex.getTable().getIdTextUpUnescape());
        node.setType(ShowType.valueOf(showIndex.getType().name()));
        this.node = node;
    }

    public ASTNode getNode() {
        return this.node;
    }
}
