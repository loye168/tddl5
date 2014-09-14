package com.taobao.tddl.monitor.unit;

import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.extension.Activate;

/**
 * 空实现,屏蔽对内部产品的依赖
 * 
 * @author jianghang 2014-2-28 下午4:21:14
 * @since 5.0.0
 */
@Activate(order = 1)
public class MockTddlRouter implements TddlRouter {

    @Override
    public void registerUnitsListener(TddlRouterUnitsListener listener) {
    }

    @Override
    public Set<String> getUnits() {
        return null;
    }

    @Override
    public String getCurrentUnit() {
        return null;
    }

    @Override
    public boolean isUnitDBUsed() {
        return false;
    }

    @Override
    public void unitDeployProtect(String appName, String sql, Map<Integer, ParameterContext> params) {
    }

    @Override
    public void unitDeployProtectWithCause(String appName, String sql, Map<Integer, ParameterContext> params) {
    }

    @Override
    public void eagleEyeRecord(boolean result, Object c) {
    }

    @Override
    public Object getValidKeyFromThread() {
        return null;
    }

    @Override
    public void clearUnitValidThreadLocal() {
    }

    @Override
    public Object getValidFromHint(String sql, Map<Integer, ParameterContext> params) {
        return null;
    }

    @Override
    public String tryRemoveUnitValidHintAndParameter(String sql, Map<Integer, ParameterContext> params) {
        return sql;
    }

    @Override
    public String replaceWithParams(String sql, Map<Integer, ParameterContext> params) {
        return sql;
    }

    @Override
    public boolean isCenterUnit() {
        return true;
    }
}
