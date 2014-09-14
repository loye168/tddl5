package com.alibaba.cobar.util;

import java.util.concurrent.locks.ReentrantLock;

/**
 * 固定容量的阻塞队列
 * 
 * @author xianmao.hexm 2010-7-21 下午07:07:36
 */
public final class FixedQueue<E> {

    private final E[] items;
    private int putIndex;
    private int takeIndex;
    private int count;
    private final ReentrantLock lock;

    @SuppressWarnings("unchecked")
    public FixedQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException();
        }
        this.items = (E[]) new Object[capacity];
        this.lock = new ReentrantLock();
    }

    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    public boolean offer(E e) {
        if (e == null) {
            throw new NullPointerException();
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (count >= items.length) {
                return false;
            } else {
                insert(e);
                return true;
            }
        } finally {
            lock.unlock();
        }
    }

    public E poll() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (count == 0) {
                return null;
            }
            return extract();
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        final E[] items = this.items;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int i = takeIndex;
            int j = count;
            while (j-- > 0) {
                items[i] = null;
                i = inc(i);
            }
            count = 0;
            putIndex = 0;
            takeIndex = 0;
        } finally {
            lock.unlock();
        }
    }

    private void insert(E x) {
        items[putIndex] = x;
        putIndex = inc(putIndex);
        ++count;
    }

    private E extract() {
        E[] items = this.items;
        int i = takeIndex;
        E x = items[i];
        items[i] = null;
        takeIndex = inc(i);
        --count;
        return x;
    }

    private int inc(int i) {
        return (++i == items.length) ? 0 : i;
    }

}
