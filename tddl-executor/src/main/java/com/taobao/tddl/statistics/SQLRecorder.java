package com.taobao.tddl.statistics;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * SQL统计排序记录器
 * 
 * @author xianmao.hexm 2010-9-30 上午10:48:28
 */
public final class SQLRecorder {

    private int                 index;
    // SQL执行时间的最小值
    private long                minValue;
    private final int           count;
    private final int           lastIndex;
    // 记录的SQL总数
    private final SQLRecord[]   records;
    private final ReentrantLock lock;
    private boolean             duplicate = true;

    public SQLRecorder(int count, boolean duplicate){
        this.count = count;
        this.lastIndex = count - 1;
        this.records = new SQLRecord[count];
        this.lock = new ReentrantLock();
        this.duplicate = duplicate;
    }

    public SQLRecord[] getRecords() {
        return records;
    }

    /**
     * 检查当前的值能否进入排名
     */
    public boolean check(long value) {
        return (index < count) || (value > minValue);
    }

    public void add(SQLRecord record) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {

            record.statement = OptimizerUtils.parameterize(record.statement);
            if (index < count) {
                records[index++] = record;
                if (index == count) {
                    Arrays.sort(records);
                    minValue = records[0].executeTime;
                }
            } else {
                swap(record);
            }
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            for (int i = 0; i < count; i++) {
                records[i] = null;
            }
            index = 0;
            minValue = 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 交换元素位置并重新定义最小值
     */
    private void swap(SQLRecord record) {
        int x = find(record.executeTime, 0, lastIndex);
        switch (x) {
            case 0:
                break;
            case 1:
                minValue = record.executeTime;
                records[0] = record;
                break;
            default:
                --x;// 向左移动一格
                final SQLRecord[] records = this.records;
                for (int i = 0; i < x; i++) {
                    records[i] = records[i + 1];
                }
                records[x] = record;
                minValue = records[0].executeTime;
        }
    }

    /**
     * 定位v在当前范围内的排名
     */
    private int find(long v, int from, int to) {
        int x = from + ((to - from + 1) >> 1);
        if (v <= records[x].executeTime) {
            --x;// 向左移动一格
            if (from >= x) {
                return v <= records[from].executeTime ? from : from + 1;
            } else {
                return find(v, from, x);
            }
        } else {
            ++x;// 向右移动一格
            if (x >= to) {
                return v <= records[to].executeTime ? to : to + 1;
            } else {
                return find(v, x, to);
            }
        }
    }

    /**
     * 记录sql执行信息
     */
    public void recordSql(String sql, long startTime, String host, String schema) {
        long time = System.currentTimeMillis() - startTime;
        if (this.check(time)) {

            if (!duplicate) {
                sql = OptimizerUtils.parameterize(sql);

                for (SQLRecord record : this.records) {
                    if (sql.equalsIgnoreCase(record.statement)) {
                        return;
                    }
                }
            }

            SQLRecord recorder = new SQLRecord();
            recorder.statement = sql;
            recorder.startTime = startTime;
            recorder.executeTime = time;

            this.add(recorder);
        }
    }

    /**
     * 记录sql执行信息
     */
    public void recordSql(String sql, long startTime, String groupName) {
        long time = System.currentTimeMillis() - startTime;
        if (this.check(time)) {

            if (!duplicate) {
                sql = OptimizerUtils.parameterize(sql);

                for (SQLRecord record : this.records) {
                    if (sql.equalsIgnoreCase(record.statement)) {
                        return;
                    }
                }
            }

            SQLRecord recorder = new SQLRecord();
            recorder.statement = sql;
            recorder.startTime = startTime;
            recorder.executeTime = time;
            recorder.dataNode = groupName;
            this.add(recorder);
        }
    }
}
