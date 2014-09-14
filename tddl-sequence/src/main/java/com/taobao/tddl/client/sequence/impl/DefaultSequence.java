package com.taobao.tddl.client.sequence.impl;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.tddl.client.sequence.Sequence;
import com.taobao.tddl.client.sequence.SequenceRange;
import com.taobao.tddl.client.sequence.exception.SequenceException;

/**
 * 序列默认实现
 * 
 * @author shenxun
 */
public class DefaultSequence implements Sequence {

    private final Lock             lock = new ReentrantLock();

    private DefaultSequenceDao     sequenceDao;

    /**
     * 序列名称
     */
    private String                 name;

    private volatile SequenceRange currentRange;

    public long nextValue() throws SequenceException {
        if (currentRange == null) {
            lock.lock();
            try {
                if (currentRange == null) {
                    currentRange = sequenceDao.nextRange(name);
                }
            } finally {
                lock.unlock();
            }
        }

        long value = currentRange.getAndIncrement();
        if (value == -1) {
            lock.lock();
            try {
                for (;;) {
                    if (currentRange.isOver()) {
                        currentRange = sequenceDao.nextRange(name);
                    }

                    value = currentRange.getAndIncrement();
                    if (value == -1) {
                        continue;
                    }

                    break;
                }
            } finally {
                lock.unlock();
            }
        }

        if (value < 0) {
            throw new SequenceException("Sequence value overflow, value = " + value);
        }

        return value;
    }

    @Override
    public long nextValue(int size) throws SequenceException {
        if (size > this.getSequenceDao().getStep()) {
            throw new SequenceException("batch size > sequence step step, please change batch size or sequence inner step");
        }

        if (currentRange == null) {
            lock.lock();
            try {
                if (currentRange == null) {
                    currentRange = sequenceDao.nextRange(name);
                }
            } finally {
                lock.unlock();
            }
        }

        long value = currentRange.getBatch(size);
        if (value == -1) {
            lock.lock();
            try {
                for (;;) {
                    if (currentRange.isOver()) {
                        currentRange = sequenceDao.nextRange(name);
                    }

                    value = currentRange.getBatch(size);
                    if (value == -1) {
                        continue;
                    }

                    break;
                }
            } finally {
                lock.unlock();
            }
        }

        if (value < 0) {
            throw new SequenceException("Sequence value overflow, value = " + value);
        }

        return value;
    }

    public DefaultSequenceDao getSequenceDao() {
        return sequenceDao;
    }

    public void setSequenceDao(DefaultSequenceDao sequenceDao) {
        this.sequenceDao = sequenceDao;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
