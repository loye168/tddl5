package com.alibaba.cobar.manager.dataobject.xml;

import java.util.ArrayList;
import java.util.List;

/**
 * @author haiqing.zhuhq 2011-6-17
 */
public class PropertyDO {

    private List<Integer> stopTimes;

    public PropertyDO(){
        this.stopTimes = new ArrayList<Integer>();
    }

    public List<Integer> getStopTimes() {
        return stopTimes;
    }

    public void setStopTimes(List<Integer> stopTimes) {
        this.stopTimes = stopTimes;
    }

    @Override
    public String toString() {
        if (stopTimes.size() < 1) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        int i;
        for (i = 0; i < stopTimes.size() - 1; i++) {
            sb.append(stopTimes.get(i)).append(",");
        }
        sb.append(stopTimes.get(i));
        return sb.toString();
    }

}
