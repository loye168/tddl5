package com.alibaba.cobar.manager.dataobject.cobarnode;

/**
 * (created at 2010-7-26)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 * @author wenfeng.cenwf 2011-4-15
 * @author haiqing.zhuhq 2011-7-12
 */
public class CommandStatus extends TimeStampedVO {

    private String processorId;
    private long   stmtPrepared;
    private long   stmtExecute;
    private long   query;
    private long   stmtClose;
    private long   ping;
    private long   quit;
    private long   other;
    private long   kill;
    private long   initDB;

    public String getProcessorId() {
        return processorId;
    }

    public void setProcessorId(String processorId) {
        this.processorId = processorId;
    }

    public long getStmtPrepared() {
        return stmtPrepared;
    }

    public void setStmtPrepared(long stmtPrepared) {
        this.stmtPrepared = stmtPrepared;
    }

    public long getStmtExecute() {
        return stmtExecute;
    }

    public void setStmtExecute(long stmtExecute) {
        this.stmtExecute = stmtExecute;
    }

    public long getQuery() {
        return query;
    }

    public void setQuery(long query) {
        this.query = query;
    }

    public long getStmtClose() {
        return stmtClose;
    }

    public void setStmtClose(long stmtClose) {
        this.stmtClose = stmtClose;
    }

    public long getPing() {
        return ping;
    }

    public void setPing(long ping) {
        this.ping = ping;
    }

    public long getQuit() {
        return quit;
    }

    public void setQuit(long quit) {
        this.quit = quit;
    }

    public long getOther() {
        return other;
    }

    public void setOther(long other) {
        this.other = other;
    }

    public long getKill() {
        return kill;
    }

    public void setKill(long kill) {
        this.kill = kill;
    }

    public long getInitDB() {
        return initDB;
    }

    public void setInitDB(long initDB) {
        this.initDB = initDB;
    }

}
