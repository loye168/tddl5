package com.taobao.tddl.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.client.RouteCondition.ROUTE_TYPE;
import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.common.model.ThreadLocalString;
import com.taobao.tddl.common.model.hint.DirectlyRouteCondition;
import com.taobao.tddl.common.model.hint.RuleRouteCondition;
import com.taobao.tddl.common.model.sqljep.Comparative;
import com.taobao.tddl.common.model.sqljep.ComparativeAND;
import com.taobao.tddl.common.model.sqljep.ComparativeOR;

/**
 * 方便业务调用直接接口的方法<br/>
 * 别太纠结这混乱的包定义,一切都是为了以前老的客户端package定义
 * 
 * @author jianghang 2014-6-11 下午12:20:40
 * @since 5.1.0
 */
public class RouteHelper {

    public static final int EQ  = Comparative.Equivalent;
    public static final int GT  = Comparative.GreaterThan;
    public static final int LT  = Comparative.LessThan;
    public static final int GTE = Comparative.GreaterThanOrEqual;
    public static final int LTE = Comparative.LessThanOrEqual;

    /**
     * 直接在某个库上,执行一条sql 这时候TDDL只做两件事情，第一个是协助判断是否在事务状态。 第二个事情是，进行表名的替换。
     * 
     * @param dbIndex dbIndex列表
     * @param logicTable 逻辑表名
     * @param table 实际表名
     * @param routeType 决定这个hint是在连接关闭的时候清空，还是在执行时就清空
     */
    public static void executeByDBAndTab(String dbIndex, String logicTable, ROUTE_TYPE routeType, String... tables) {
        DirectlyRouteCondition condition = new DirectlyRouteCondition();
        if (tables == null) {
            throw new IllegalArgumentException("tables is null");
        }

        for (String table : tables) {
            condition.addTable(table);
        }
        condition.setVirtualTableName(logicTable);
        condition.setDBId(dbIndex);
        condition.setRouteType(routeType);
        ThreadLocalMap.put(ThreadLocalString.DB_SELECTOR, condition);
    }

    /**
     * 直接在某个库上,执行一条sql 这时候TDDL只做两件事情，第一个是协助判断是否在事务状态。 第二个事情是，进行表名的替换。
     * 
     * @param dbIndex dbIndex列表
     * @param logicTable 逻辑表名
     * @param table 实际表名
     */
    public static void executeByDBAndTab(String dbIndex, String logicTable, String... table) {
        executeByDBAndTab(dbIndex, logicTable, ROUTE_TYPE.FLUSH_ON_EXECUTE, table);
    }

    /**
     * 直接在某个库上,执行一条sql，并允许进行多组表名的替换，主要目标是join的sql tddl should do : 1. 判断事务是否可执行。
     * 2. 将多组表名进行替换。
     * 
     * @param dbIndex dbIndex列表
     * @param tableMap 源表名->目标表名的map
     */
    public static void executeByDBAndTab(String dbIndex, Map<String, String> tableMap) {
        executeByDBAndTab(dbIndex, tableMap, ROUTE_TYPE.FLUSH_ON_EXECUTE);
    }

    /**
     * 直接在某个库上,执行一条sql，并允许进行多组表名的替换，主要目标是join的sql
     * 
     * @param dbIndex dbIndex列表
     * @param tableMap 源表名->目标表名的map
     * @param routeType 决定这个hint是在连接关闭的时候清空，还是在执行时就清空
     */
    public static void executeByDBAndTab(String dbIndex, Map<String, String> tableMap, ROUTE_TYPE routeType) {
        executeByDBAndTab(dbIndex, Arrays.asList(tableMap), routeType);
    }

    /**
     * 直接在某个库上,执行一条sql，并允许进行多组表名的替换，主要目标是join的sql
     * 
     * @param dbIndex dbIndex列表
     * @param tableMap 源表名->目标表名的map
     * @param routeType 决定这个hint是在连接关闭的时候清空，还是在执行时就清空
     */
    public static void executeByDBAndTab(String dbIndex, List<Map<String, String>> tableMap) {
        executeByDBAndTab(dbIndex, tableMap, ROUTE_TYPE.FLUSH_ON_EXECUTE);
    }

    /**
     * 直接在某个库上,执行一条sql，并允许进行多组表名的替换，主要目标是join的sql
     * 
     * @param dbIndex dbIndex列表
     * @param tableMap 源表名->目标表名的map
     * @param routeType 决定这个hint是在连接关闭的时候清空，还是在执行时就清空
     */
    public static void executeByDBAndTab(String dbIndex, List<Map<String, String>> tableMaps, ROUTE_TYPE routeType) {
        DirectlyRouteCondition condition = new DirectlyRouteCondition();
        if (tableMaps == null) {
            throw new IllegalArgumentException("tableMap is null");
        }

        String logicTable = null;
        for (Map<String, String> tableMap : tableMaps) {
            StringBuilder vtabs = new StringBuilder();
            StringBuilder rtabs = new StringBuilder();
            int size = tableMap.size();
            int i = 1;
            for (Map.Entry<String, String> entry : tableMap.entrySet()) {
                vtabs.append(entry.getKey());
                rtabs.append(entry.getValue());
                if (i < size) {
                    vtabs.append(',');
                    rtabs.append(',');
                }
            }

            if (logicTable == null) {
                logicTable = vtabs.toString();
            } else if (!logicTable.equalsIgnoreCase(vtabs.toString())) {
                throw new IllegalArgumentException("多表替换时逻辑表出现不一致");
            }
            condition.addTable(rtabs.toString());
        }

        condition.setVirtualTableName(logicTable);
        condition.setDBId(dbIndex);
        condition.setRouteType(routeType);
        ThreadLocalMap.put(ThreadLocalString.DB_SELECTOR, condition);
    }

    /**
     * 根据db index 执行一条sql. sql就是你通过Ibatis输入的sql. 只做一件事情，就是协助判断是否需要进行事务
     * 
     * @param dbIndex dbIndex列表
     * @param routeType 决定这个hint是在连接关闭的时候清空，还是在执行时就清空
     */
    public static void executeByDB(String dbIndex, ROUTE_TYPE routeType) {
        DirectlyRouteCondition condition = new DirectlyRouteCondition();
        condition.setDBId(dbIndex);
        condition.setRouteType(routeType);
        ThreadLocalMap.put(ThreadLocalString.DB_SELECTOR, condition);
    }

    /**
     * 根据db index 执行一条sql. sql就是你通过Ibatis输入的sql. 只做一件事情，就是协助判断是否需要进行事务
     * 
     * @param dbIndex dbIndex列表
     */
    public static void executeByDB(String dbIndex) {
        executeByDB(dbIndex, ROUTE_TYPE.FLUSH_ON_EXECUTE);
    }

    // ===================== 基于规则的hint ===========================

    /**
     * 根据条件选择数据库进行执行sql
     * 
     * @param logicTable
     * @param key
     * @param comp
     */
    public static void executeByCondition(String logicTable, String key, Comparable<?> comp) {
        executeByCondition(logicTable, key, comp, ROUTE_TYPE.FLUSH_ON_EXECUTE);
    }

    /**
     * 根据条件选择数据库进行执行sql
     * 
     * @param logicTable
     * @param key
     * @param comp
     * @param routeType
     */
    public static void executeByCondition(String logicTable, String key, Comparable<?> comp, ROUTE_TYPE routeType) {
        RuleRouteCondition simpleCondition = new RuleRouteCondition();
        simpleCondition.setVirtualTableName(logicTable);
        simpleCondition.put(key, comp);
        simpleCondition.setRouteType(routeType);
        ThreadLocalMap.put(ThreadLocalString.ROUTE_CONDITION, simpleCondition);
    }

    /**
     * 指定多列的条件
     * 
     * @param logicTable
     * @param param
     * @param routeType
     */
    public static void executeByAdvancedCondition(String logicTable, Map<String, Comparable<?>> param,
                                                  ROUTE_TYPE routeType) {
        RuleRouteCondition condition = new RuleRouteCondition();
        condition.setVirtualTableName(logicTable);
        for (Map.Entry<String, Comparable<?>> entry : param.entrySet()) {
            condition.put(entry.getKey(), entry.getValue());
        }
        condition.setRouteType(routeType);
        ThreadLocalMap.put(ThreadLocalString.ROUTE_CONDITION, condition);
    }

    /**
     * 指定多列的条件
     * 
     * @param logicTable
     * @param param
     */
    public static void executeByAdvancedCondition(String logicTable, Map<String, Comparable<?>> param) {
        executeByAdvancedCondition(logicTable, param, ROUTE_TYPE.FLUSH_ON_EXECUTE);
    }

    /**
     * 构建or条件
     * 
     * @param parent
     * @param target
     * @return
     */
    public static Comparative or(Comparative parent, Comparative target) {
        if (parent == null) {
            ComparativeOR or = new ComparativeOR();
            or.addComparative(target);
            return or;
        } else {
            if (parent instanceof ComparativeOR) {
                ((ComparativeOR) parent).addComparative(target);
                return parent;
            } else {
                ComparativeOR or = new ComparativeOR();
                or.addComparative(parent);
                or.addComparative(target);
                return or;
            }
        }
    }

    /**
     * 构建and条件
     * 
     * @param parent
     * @param target
     * @return
     */
    public static Comparative and(Comparative parent, Comparative target) {
        if (parent == null) {
            ComparativeAND and = new ComparativeAND();
            and.addComparative(target);
            return and;
        } else {
            if (parent instanceof ComparativeAND) {

                ComparativeAND and = ((ComparativeAND) parent);
                if (and.getList().size() == 1) {
                    and.addComparative(target);
                    return and;
                } else {
                    ComparativeAND andNew = new ComparativeAND();
                    andNew.addComparative(and);
                    andNew.addComparative(target);
                    return andNew;
                }

            } else {
                ComparativeAND and = new ComparativeAND();
                and.addComparative(parent);
                and.addComparative(target);
                return and;
            }
        }
    }
}
