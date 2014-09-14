package com.taobao.tddl.monitor.unit;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.jdbc.ParameterContext;

public interface TddlRouter {

    public void registerUnitsListener(TddlRouterUnitsListener listener);

    public Set<String> getUnits();

    public String getCurrentUnit();

    public boolean isUnitDBUsed();

    public void unitDeployProtect(String appName, String sql, Map<Integer, ParameterContext> params)
                                                                                                    throws SQLException;

    public void unitDeployProtectWithCause(String appName, String sql, Map<Integer, ParameterContext> params)
                                                                                                             throws UnitDeployInvalidException,
                                                                                                             SQLException;

    public void eagleEyeRecord(boolean result, Object c) throws UnitDeployInvalidException;

    public Object getValidKeyFromThread();

    public void clearUnitValidThreadLocal();

    public Object getValidFromHint(String sql, Map<Integer, ParameterContext> params) throws SQLException;

    public String tryRemoveUnitValidHintAndParameter(String sql, Map<Integer, ParameterContext> params);

    public String replaceWithParams(String sql, Map<Integer, ParameterContext> params) throws SQLException;

    /**
     * 提供给一些场景判断当前是否在中心的全量业务机房 >> 例如有些场景需要根据这个决定是否启动定时任务等
     * 
     * @return boolean 在没启用单元规则时全部返回true
     */
    public boolean isCenterUnit();

}
