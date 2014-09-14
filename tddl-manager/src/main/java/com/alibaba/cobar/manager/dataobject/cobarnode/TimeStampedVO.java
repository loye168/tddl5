package com.alibaba.cobar.manager.dataobject.cobarnode;

import com.alibaba.fastjson.JSONObject;

/**
 * (created at 2010-7-26)
 * 
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public abstract class TimeStampedVO {

    private long sampleTimeStamp;

    public long getSampleTimeStamp() {
        return sampleTimeStamp;
    }

    public void setSampleTimeStamp(long sampleTimeStamp) {
        this.sampleTimeStamp = sampleTimeStamp;
    }

    public String toJSONString() {
        return JSONObject.toJSONString(this);
    }
}
