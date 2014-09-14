package com.taobao.tddl.monitor.unit;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;

public class RouterUnitsHelper {

    static TddlRouter delegate = null;
    static {
        delegate = ExtensionLoader.load(TddlRouter.class);
    }

    public static void registerUnitsListener(TddlRouterUnitsListener listener) {
        delegate.registerUnitsListener(listener);
    }

    public static Set<String> getUnits() {
        return delegate.getUnits();
    }

    public static String getCurrentUnit() {
        return delegate.getCurrentUnit();
    }

    public static boolean isUnitDBUsed() {
        return delegate.isUnitDBUsed();
    }

    public static void unitDeployProtect(String appName, String sql, Map<Integer, ParameterContext> params)
                                                                                                           throws SQLException {
        delegate.unitDeployProtect(appName, sql, params);
    }

    public static void unitDeployProtect(String appName, String sql) throws SQLException {
        unitDeployProtect(appName, sql, null);
    }

    public static void unitDeployProtect(String appName) throws SQLException {
        unitDeployProtect(appName, null);
    }

    protected static void unitDeployProtectWithCause(String appName, String sql, Map<Integer, ParameterContext> params)
                                                                                                                       throws UnitDeployInvalidException,
                                                                                                                       SQLException {

        delegate.unitDeployProtectWithCause(appName, sql, params);
    }

    protected static void eagleEyeRecord(boolean result, Object c) throws UnitDeployInvalidException {

        delegate.eagleEyeRecord(result, c);
    }

    protected static Object getValidKeyFromThread() {

        return delegate.getValidKeyFromThread();

    }

    public static void clearUnitValidThreadLocal() {
        delegate.clearUnitValidThreadLocal();
    }

    public static Object getValidFromHint(String sql, Map<Integer, ParameterContext> params) throws SQLException {
        return delegate.getValidFromHint(sql, params);
    }

    public static boolean isCenterUnit() {
        return delegate.isCenterUnit();
    }

    public static String tryRemoveUnitValidHintAndParameter(String sql) {
        return tryRemoveUnitValidHintAndParameter(sql, null);
    }

    protected static String tryRemoveUnitValidHintAndParameter(String sql, Map<Integer, ParameterContext> params) {
        return delegate.tryRemoveUnitValidHintAndParameter(sql, params);
    }

    protected static String replaceWithParams(String sql, Map<Integer, ParameterContext> params) throws SQLException {
        return delegate.replaceWithParams(sql, params);
    }

}
