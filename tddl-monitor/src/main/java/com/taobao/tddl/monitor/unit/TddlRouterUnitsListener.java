package com.taobao.tddl.monitor.unit;

/**
 * 屏蔽对route unit的直接依赖
 * 
 * @author jianghang 2014-2-28 下午4:14:59
 * @since 5.0.0
 */
public interface TddlRouterUnitsListener {

    public enum STATUS {
        BEGIN, END
    };

    public boolean onChanged(STATUS status);
}
