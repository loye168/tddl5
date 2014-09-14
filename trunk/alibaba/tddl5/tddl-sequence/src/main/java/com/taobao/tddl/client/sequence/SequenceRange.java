package com.taobao.tddl.client.sequence;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 序列区间
 * 
 * @author nianbing
 */
public class SequenceRange {

    private final long       min;
    private final long       max;

    private final AtomicLong value;

    private volatile boolean over = false;

    public SequenceRange(long min, long max){
        this.min = min;
        this.max = max;
        this.value = new AtomicLong(min);
    }

    public long getBatch(int size) {
        long currentValue = value.getAndAdd(size) + size - 1;
        if (currentValue > max) {
            over = true;
            return -1;
        }

        return currentValue;
    }

    public long getAndIncrement() {
        long currentValue = value.getAndIncrement();
        if (currentValue > max) {
            over = true;
            return -1;
        }

        return currentValue;
    }

    public long getMin() {
        return min;
    }

    public long getMax() {
        return max;
    }

    public boolean isOver() {
        return over;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("max: ").append(max).append(", min: ").append(min).append(", value: ").append(value);
        return sb.toString();
    }

}
