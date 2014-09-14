package com.alibaba.cobar.manager.dataobject.cobarnode;

/**
 * (created at 2010-8-9)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 * @author wenfeng.cenwf 2011-4-15
 */
public class ProcessorStatus extends TimeStampedVO {

    private String processorId;
    private int    rQueue;
    private long   requestCount;
    private int    wQueue;
    /** Byte */
    private long   netIn;
    /** Byte */
    private long   netOut;
    /** connection count */
    private int    connections;

    private long   freeBuffer;
    private long   totalBuffer;
    private long   bc_count;

    public String getProcessorId() {
        return processorId;
    }

    public void setProcessorId(String processorId) {
        this.processorId = processorId;
    }

    public long getNetIn() {
        return netIn;
    }

    public void setNetIn(long netIn) {
        this.netIn = netIn;
    }

    public long getNetOut() {
        return netOut;
    }

    public void setNetOut(long netOut) {
        this.netOut = netOut;
    }

    public int getrQueue() {
        return rQueue;
    }

    public void setrQueue(int rQueue) {
        this.rQueue = rQueue;
    }

    public long getRequestCount() {
        return requestCount;
    }

    public void setRequestCount(long requestCount) {
        this.requestCount = requestCount;
    }

    public long getFreeBuffer() {
        return freeBuffer;
    }

    public void setFreeBuffer(long freeBuffer) {
        this.freeBuffer = freeBuffer;
    }

    public long getTotalBuffer() {
        return totalBuffer;
    }

    public void setTotalBuffer(long totalBuffer) {
        this.totalBuffer = totalBuffer;
    }

    public int getwQueue() {
        return wQueue;
    }

    public void setwQueue(int wQueue) {
        this.wQueue = wQueue;
    }

    public int getConnections() {
        return connections;
    }

    public void setConnections(int connections) {
        this.connections = connections;
    }

    public long getBc_count() {
        return bc_count;
    }

    public void setBc_count(long bc_count) {
        this.bc_count = bc_count;
    }

}
