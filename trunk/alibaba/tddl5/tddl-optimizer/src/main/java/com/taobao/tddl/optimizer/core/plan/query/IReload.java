package com.taobao.tddl.optimizer.core.plan.query;

import com.taobao.tddl.optimizer.core.ast.reload.ReloadNode.ReloadType;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public interface IReload extends IDataNodeExecutor<IReload> {

    ReloadType getType();

    void setType(ReloadType type);
}
