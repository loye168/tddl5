package com.taobao.tddl.optimizer.core.ast.dml;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;

/**
 * @author mengshi.sunmengshi 2014年5月15日 下午4:10:20
 * @since 5.1.0
 */
public class LoadDataNode extends ASTNode {

    private String tableName;

    public LoadDataNode(String tableName){
        super();
        this.tableName = tableName;
    }

    @Override
    public void build() {
        if (tableName == null) {
            throw new IllegalArgumentException("tableName is null");
        }

        OptimizerContext.getContext().getSchemaManager().getTable(tableName);
    }

    @Override
    public IDataNodeExecutor toDataNodeExecutor(int shareIndex) {
        IPut put = ASTNodeFactory.getInstance().createReplace();
        put.setSql(this.getSql());
        put.setTableName(this.tableName);
        put.executeOn(this.getDataNode());
        return put;
    }

    @Override
    public void assignment(Parameters parameterSettings) {

    }

    @Override
    public boolean isNeedBuild() {
        return false;
    }

    @Override
    public String toString(int inden, int shareIndex) {
        return this.sql;
    }

    @Override
    public IFunction getNextSubqueryOnFilter() {
        return null;
    }

    @Override
    public LoadDataNode copy() {
        return deepCopy();
    }

    @Override
    public LoadDataNode copySelf() {
        return deepCopy();
    }

    @Override
    public LoadDataNode deepCopy() {
        LoadDataNode node = new LoadDataNode(tableName);
        node.setSql(this.sql);
        node.executeOn(this.getDataNode());
        return node;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

}
