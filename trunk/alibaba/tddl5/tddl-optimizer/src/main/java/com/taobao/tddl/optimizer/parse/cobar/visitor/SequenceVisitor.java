package com.taobao.tddl.optimizer.parse.cobar.visitor;

import java.sql.Timestamp;

import com.alibaba.cobar.parser.ast.stmt.dal.ShowSequences;
import com.alibaba.cobar.parser.ast.stmt.ddl.CreateSequence;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;

public class SequenceVisitor extends EmptySQLASTVisitor {

    private InsertNode createSequenceInsert;
    private TableNode  showSequenceSelect;

    public SequenceVisitor(){

    }

    @Override
    public void visit(CreateSequence node) {
        TableNode table = getTableNode();
        String insertColumns = this.getInsertColumnsStr();

        Object[] iv = getRowValue(node);
        this.createSequenceInsert = table.insert(insertColumns, iv);
    }

    private TableNode getTableNode() {
        TableNode table = null;
        table = new TableNode("SEQUENCE");
        return table;
    }

    @Override
    public void visit(ShowSequences node) {
        this.showSequenceSelect = getTableNode();
    }

    private String getInsertColumnsStr() {
        return "name value gmt_modified";
    }

    private Object[] getRowValue(CreateSequence node) {
        Object[] iv = new Object[3];

        iv[0] = node.getName().getIdText();
        iv[1] = node.getStart().longValue();
        iv[2] = new Timestamp(System.currentTimeMillis());

        return iv;
    }

    public InsertNode getCreateSequenceInsert() {
        return createSequenceInsert;
    }

    public QueryTreeNode getShowSequencesSelect() {
        return this.showSequenceSelect;
    }
}
