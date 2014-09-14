package com.alibaba.cobar.net.util;

/**
 * 弱精度的计时器，考虑性能不使用同步策略。
 * 
 * @author xianmao.hexm 2011-1-18 下午06:10:55
 */
public class TimeUtil {

    private static long CURRENT_TIME = System.currentTimeMillis();

    public static final long currentTimeMillis() {
        return CURRENT_TIME;
    }

    public static final void update() {
        CURRENT_TIME = System.currentTimeMillis();
    }

}
