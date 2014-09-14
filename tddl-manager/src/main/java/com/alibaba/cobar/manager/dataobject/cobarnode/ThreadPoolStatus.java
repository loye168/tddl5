package com.alibaba.cobar.manager.dataobject.cobarnode;

/**
 * (created at 2010-7-26)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 * @author wenfeng.cenwf 2011-4-15
 */
public class ThreadPoolStatus {

    private String threadPoolName;
    private int    poolSize;
    private int    activeSize;
    private int    taskQueue;
    private long   completedTask;
    private long   totalTask;

    public String getThreadPoolName() {
        return threadPoolName;
    }

    public void setThreadPoolName(String threadPoolName) {
        this.threadPoolName = threadPoolName;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public int getActiveSize() {
        return activeSize;
    }

    public void setActiveSize(int activeSize) {
        this.activeSize = activeSize;
    }

    public int getTaskQueue() {
        return taskQueue;
    }

    public void setTaskQueue(int taskQueue) {
        this.taskQueue = taskQueue;
    }

    public long getCompletedTask() {
        return completedTask;
    }

    public void setCompletedTask(long completedTask) {
        this.completedTask = completedTask;
    }

    public long getTotalTask() {
        return totalTask;
    }

    public void setTotalTask(long totalTask) {
        this.totalTask = totalTask;
    }
}
