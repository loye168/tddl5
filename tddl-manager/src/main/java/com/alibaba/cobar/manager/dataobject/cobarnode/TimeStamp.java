package com.alibaba.cobar.manager.dataobject.cobarnode;

import com.alibaba.cobar.manager.util.FormatUtil;

/**
 * @author wenfeng.cenwf 2011-4-14
 */
public class TimeStamp {

    private long timestamp;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getFormatTime() {
        return FormatUtil.fromMilliseconds2String(this.timestamp);
    }
}
