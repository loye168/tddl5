package com.taobao.tddl.optimizer.sequence;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;

/**
 * sequence proxy实现,基于extenstion查找具体实现
 * 
 * @author jianghang 2014-4-28 下午5:09:15
 * @since 5.1.0
 */
public class SequenceManagerProxy extends AbstractLifecycle implements ISequenceManager {

    private static ISequenceManager instance;
    private ISequenceManager        delegate;

    public static ISequenceManager getInstance() {
        if (instance == null) {
            synchronized (SequenceManagerProxy.class) {
                if (instance == null) {
                    instance = new SequenceManagerProxy();
                    try {
                        instance.init();
                    } catch (TddlException e) {
                        throw new TddlNestableRuntimeException(e);
                    }
                }
            }
        }

        return instance;
    }

    @Override
    protected void doInit() throws TddlException {
        delegate = ExtensionLoader.load(ISequenceManager.class);
        delegate.init();
    }

    @Override
    protected void doDestroy() throws TddlException {
        if (delegate != null) {
            delegate.destroy();
        }
    }

    @Override
    public Long nextValue(String seqName) {
        return delegate.nextValue(seqName);
    }

    @Override
    public Long nextValue(String seqName, int batchSize) {
        return delegate.nextValue(seqName, batchSize);
    }

}
