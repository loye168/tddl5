package com.taobao.tddl.executor.common;

import com.taobao.tddl.client.sequence.Sequence;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.optimizer.sequence.ISequenceManager;

public abstract class AbstractSequenceManager extends AbstractLifecycle implements ISequenceManager {

    protected abstract Sequence getSequence(String name);

    @Override
    public Long nextValue(String seqName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long nextValue(String seqName, int batchSize) {
        throw new UnsupportedOperationException();
    }

}
