package com.taobao.tddl.config.impl.mock;

import java.util.List;
import java.util.concurrent.Executor;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.UnitConfigDataHandler;

/**
 * 假的实现，保证可脱离diamond启动
 * 
 * @author jianghang 2014-2-27 下午6:52:04
 * @since 5.0.0
 */
@Activate(name = "mock", order = 1)
public class MockUnitConfigDataHandler extends UnitConfigDataHandler {

    @Override
    public String getData(long timeout, String strategy) {
        throw new NotSupportException();
    }

    @Override
    public String getNullableData(long timeout, String strategy) {
        throw new NotSupportException();
    }

    @Override
    public void addListener(ConfigDataListener configDataListener, Executor executor) {
        throw new NotSupportException();
    }

    @Override
    public void addListeners(List<ConfigDataListener> configDataListenerList, Executor executor) {
        throw new NotSupportException();
    }

}
