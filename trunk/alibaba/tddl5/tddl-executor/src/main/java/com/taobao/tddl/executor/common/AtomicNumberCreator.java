package com.taobao.tddl.executor.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Description: 简单自增序列号生成器
 * 
 * @author: qihao
 * @version: 1.0 Filename: AtomicNumberCreator.java Create at: Aug 10, 2010
 * 3:13:10 PM Copyright: Copyright (c)2010 Company: TaoBao Modification History:
 * Date Author Version Description
 * ------------------------------------------------------------------ Aug 10,
 * 2010 qihao 1.0 1.0 Version
 */
public class AtomicNumberCreator {

    /**
     * int序号自增器，由于复位时需要进行DLC，所以这里是volatile的
     */
    private volatile AtomicInteger integerNumber = new AtomicInteger(0);

    private final ReentrantLock    integerLock   = new ReentrantLock();

    /**
     * long序号自增器，由于复位时需要进行DLC，所以这里是volatile的
     */
    private volatile AtomicLong    longNumber    = new AtomicLong(0);

    private final ReentrantLock    longLock      = new ReentrantLock();

    private AtomicNumberCreator(){
    }

    public static AtomicNumberCreator getNewInstance() {
        return new AtomicNumberCreator();
    }

    /**
     * 生成int的自增数字,从1开始自增，当达到Integer.MAX_VALUE 时会恢复到初始值1
     * 
     * @return
     */
    public int getIntegerNextNumber() {
        int num = integerNumber.incrementAndGet();
        if (num < 0) {
            // 为了保证多线程复位原子性进行DCL双检查锁
            integerLock.lock();
            try {
                if (integerNumber.get() < 0) {
                    // DCL双检查锁通过后对AtomicInteger进行复位
                    integerNumber.set(0);
                }
                return integerNumber.incrementAndGet();
            } finally {
                integerLock.unlock();
            }
        }
        return num;
    }

    /**
     * 生成long的自增数字,从1开始自增，当达到Long.MAX_VALUE 时会恢复到初始值1
     * 
     * @return
     */
    public long getLongNextNumber() {
        long num = longNumber.incrementAndGet();
        // 为了保证多线程复位原子性进行DCL双检查锁
        if (num < 0) {
            longLock.lock();
            try {
                if (longNumber.get() < 0) {
                    // DCL双检查锁通过后对AtomicLong进行复位
                    longNumber.set(0);
                }
                return longNumber.incrementAndGet();
            } finally {
                longLock.unlock();
            }
        }
        return longNumber.get();
    }
}
