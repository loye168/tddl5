package com.taobao.tddl.config.impl.mock;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.utils.extension.Activate;
import com.taobao.tddl.config.impl.holder.AbstractConfigDataHolder;

/**
 * 假的实现，保证可脱离diamond启动
 * 
 * @author jianghang 2014-2-27 下午6:53:23
 * @since 5.0.0
 */
@Activate(name = "mock", order = 1)
public class MockConfigHolder extends AbstractConfigDataHolder {

    @Override
    public Map<String, String> getData(List<String> dataIds) {
        return Collections.EMPTY_MAP;
    }

    @Override
    protected Map<String, String> queryAndHold(List<String> dataIds, String unitName) {
        return Collections.EMPTY_MAP;
    }

    @Override
    protected void addDatas(Map<String, String> confMap) {
        throw new NotSupportException();
    }

    @Override
    public String getData(String dataId) {
        return configHouse.containsKey(dataId) ? configHouse.get(dataId) : getDataFromSonHolder(dataId);
    }

}
