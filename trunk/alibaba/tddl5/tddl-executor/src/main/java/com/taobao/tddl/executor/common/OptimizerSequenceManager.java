package com.taobao.tddl.executor.common;

import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.optimizer.sequence.ISequenceManager;

/**
 * 基于tddl sequence的实现
 * 
 * @author jianghang 2014-4-28 下午9:04:00
 * @since 5.1.0
 */
@Activate(order = 1)
public class OptimizerSequenceManager extends AbstractLifecycle implements ISequenceManager {

    @Override
    public Long nextValue(String seqName) {
        return ExecutorContext.getContext().getSeqeunceManager().nextValue(seqName);
    }

    @Override
    public Long nextValue(String seqName, int batchSize) {
        return ExecutorContext.getContext().getSeqeunceManager().nextValue(seqName, batchSize);
    }

}
