package com.taobao.tddl.statistics;

import java.util.ArrayList;
import java.util.List;

public class SQLTracer {

    List<SQLOperation> ops = new ArrayList();

    public void trace(SQLOperation oper) {
        ops.add(oper);
    }

    public List<SQLOperation> getOperations() {
        return this.ops;
    }
}
