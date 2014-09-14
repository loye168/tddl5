package com.taobao.tddl.client.sequence.impl;

import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.client.sequence.Sequence;
import com.taobao.tddl.client.sequence.SequenceRange;
import com.taobao.tddl.client.sequence.exception.SequenceException;
import com.taobao.tddl.monitor.eagleeye.EagleeyeHelper;

public class GroupSequence implements Sequence {

    private final Lock             lock = new ReentrantLock();
    private GroupSequenceDao       sequenceDao;

    private String                 name;
    private volatile SequenceRange currentRange;
    // 全链路压测需求
    private volatile SequenceRange testCurrentRange;

    /**
     * 初始化一下，如果name不存在，则给其初始值<br>
     * 
     * @throws SequenceException
     * @throws SQLException
     */
    public void init() throws SequenceException, SQLException {
        synchronized (this) // 为了保证安全，
        {
            sequenceDao.adjust(name);
        }
    }

    @Override
    public long nextValue() throws SequenceException {
        boolean isTest = this.isTestSeq();
        if (getSequenceRange(isTest) == null) {
            lock.lock();
            try {
                if (getSequenceRange(isTest) == null) {
                    setSequenceRange(sequenceDao.nextRange(name), isTest);
                }
            } finally {
                lock.unlock();
            }
        }

        long value = getSequenceRange(isTest).getAndIncrement();
        if (value == -1) {
            lock.lock();
            try {
                for (;;) {
                    if (getSequenceRange(isTest).isOver()) {
                        setSequenceRange(sequenceDao.nextRange(name), isTest);
                    }

                    value = getSequenceRange(isTest).getAndIncrement();
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

    public long nextValue(int size) throws SequenceException {
        if (size > this.getSequenceDao().getInnerStep()) {
            throw new SequenceException("batch size > sequence step step, please change batch size or sequence inner step");
        }

        boolean isTest = this.isTestSeq();

        if (getSequenceRange(isTest) == null) {
            lock.lock();
            try {
                if (getSequenceRange(isTest) == null) {
                    setSequenceRange(sequenceDao.nextRange(name), isTest);
                }
            } finally {
                lock.unlock();
            }
        }

        long value = getSequenceRange(isTest).getBatch(size);

        if (value == -1) {
            lock.lock();
            try {
                for (;;) {
                    if (getSequenceRange(isTest).isOver()) {
                        setSequenceRange(sequenceDao.nextRange(name), isTest);
                    }

                    value = getSequenceRange(isTest).getBatch(size);

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

    private SequenceRange getSequenceRange(boolean isTest) {
        if (isTest) {
            return this.testCurrentRange;
        } else {
            return this.currentRange;
        }
    }

    private void setSequenceRange(SequenceRange range, boolean isTest) {
        if (isTest) {
            this.testCurrentRange = range;
        } else {
            this.currentRange = range;
        }
    }

    private boolean isTestSeq() {
        String t = EagleeyeHelper.getUserData("t");
        if (!StringUtils.isBlank(t) && t.equals("1")) {
            return true;
        } else {
            return false;
        }
    }

    public GroupSequenceDao getSequenceDao() {
        return sequenceDao;
    }

    public void setSequenceDao(GroupSequenceDao sequenceDao) {
        this.sequenceDao = sequenceDao;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
