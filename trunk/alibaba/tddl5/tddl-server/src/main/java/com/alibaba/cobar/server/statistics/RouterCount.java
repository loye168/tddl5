package com.alibaba.cobar.server.statistics;

/**
 * @author xianmao.hexm 2010-9-30 上午10:43:58
 */
public final class RouterCount {

    private long routeCount;
    private long timeCount;
    private long maxRouteTime;
    private long maxRouteSQL;

    public void doRoute(long sqlId, long time) {
        routeCount++;
        timeCount += time;
        if (time > maxRouteTime) {
            maxRouteTime = time;
            maxRouteSQL = sqlId;
        }
    }

    public long getRouteCount() {
        return routeCount;
    }

    public long getTimeCount() {
        return timeCount;
    }

    public long getMaxRouteTime() {
        return maxRouteTime;
    }

    public long getMaxRouteSQL() {
        return maxRouteSQL;
    }

}
