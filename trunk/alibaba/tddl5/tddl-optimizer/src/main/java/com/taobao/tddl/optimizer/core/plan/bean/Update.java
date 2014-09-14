package com.taobao.tddl.optimizer.core.plan.bean;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.plan.dml.IUpdate;

public class Update extends Put<IUpdate> implements IUpdate {

    public Update(){
        putType = PUT_TYPE.UPDATE;
    }

    @Override
    public IUpdate copy() {
        IUpdate update = ASTNodeFactory.getInstance().createUpdate();
        copySelfTo(update);
        return update;
    }
}
