package com.taobao.tddl.optimizer.core.ast.reload;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.bean.Reload;

public class ReloadNode extends ASTNode {

    public enum ReloadType {
        SCHEMA;
    }

    protected ReloadType type;

    public ReloadType getType() {
        return type;
    }

    public void setType(ReloadType type) {
        this.type = type;
    }

    @Override
    public void build() {

    }

    @Override
    public IDataNodeExecutor toDataNodeExecutor(int shareIndex) {
        Reload reload = new Reload();
        reload.setType(this.type);
        return reload;
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
        return "RELOAD SCHEMA";
    }

    @Override
    public IFunction getNextSubqueryOnFilter() {
        return null;
    }

    @Override
    public ReloadNode copy() {
        ReloadNode newReload = new ReloadNode();
        newReload.setType(type);
        return newReload;
    }

    @Override
    public ReloadNode copySelf() {
        return copy();
    }

    @Override
    public ReloadNode deepCopy() {
        return copy();
    }

}
