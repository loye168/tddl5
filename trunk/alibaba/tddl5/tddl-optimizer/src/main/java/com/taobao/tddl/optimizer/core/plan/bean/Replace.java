package com.taobao.tddl.optimizer.core.plan.bean;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.plan.dml.IReplace;

public class Replace extends Put<IReplace> implements IReplace {

    public Replace(){
        putType = PUT_TYPE.REPLACE;
    }

    @Override
    public IReplace copy() {
        IReplace replace = ASTNodeFactory.getInstance().createReplace();
        copySelfTo(replace);
        return replace;
    }
}
