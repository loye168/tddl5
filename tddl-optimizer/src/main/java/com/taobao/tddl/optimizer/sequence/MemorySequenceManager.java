package com.taobao.tddl.optimizer.sequence;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;

/**
 * 简单的基于内存实现
 * 
 * @author jianghang 2014-4-28 下午5:33:24
 * @since 5.1.0
 */
public class MemorySequenceManager extends AbstractLifecycle implements ISequenceManager {

    private LoadingCache<String, AtomicLong> sequence = CacheBuilder.newBuilder()
                                                          .build(new CacheLoader<String, AtomicLong>() {

                                                              @Override
                                                              public AtomicLong load(String key) throws Exception {
                                                                  return new AtomicLong(1);
                                                              }
                                                          });

    @Override
    public Long nextValue(String seqName) {
        try {
            return sequence.get(seqName).getAndIncrement();
        } catch (ExecutionException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    @Override
    public Long nextValue(String seqName, int batchSize) {
        try {
            return sequence.get(seqName).getAndAdd(batchSize) + batchSize - 1;
        } catch (ExecutionException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }
}
