package com.taobao.tddl.optimizer.utils;

import java.util.concurrent.atomic.AtomicLong;

public class UniqIdGen {

    private static AtomicLong currentId  = new AtomicLong(0L);
    private static AtomicLong subqueryId = new AtomicLong(0L);

    public static long genRequestID() {
        return currentId.addAndGet(1L);
    }

    public static long genSubqueryID() {
        return subqueryId.addAndGet(1L);
    }

}
