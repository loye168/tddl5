package com.taobao.tddl.optimizer.core.plan.bean;

import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.ast.reload.ReloadNode.ReloadType;
import com.taobao.tddl.optimizer.core.plan.query.IReload;

public class Reload extends DataNodeExecutor<IReload> implements IReload {

    private ReloadType type;

    @Override
    public String toStringWithInden(int inden, ExplainMode mode) {
        return "RELOAD " + type == null ? "" : type.toString();
    }

    @Override
    public IReload copy() {
        Reload newR = new Reload();
        newR.setType(this.type);

        return newR;
    }

    @Override
    public void accept(PlanVisitor visitor) {

    }

    @Override
    public ReloadType getType() {
        return this.type;
    }

    @Override
    public void setType(ReloadType type) {
        this.type = type;
    }

}
