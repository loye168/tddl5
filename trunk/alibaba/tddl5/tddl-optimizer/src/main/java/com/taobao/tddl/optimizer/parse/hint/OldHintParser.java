package com.taobao.tddl.optimizer.parse.hint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSONException;
import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.common.model.hint.DirectlyRouteCondition;
import com.taobao.tddl.common.model.hint.RuleRouteCondition;
import com.taobao.tddl.common.model.sqljep.Comparative;
import com.taobao.tddl.optimizer.exception.OptimizerException;
import com.taobao.tddl.optimizer.exception.SqlParserException;
import com.taobao.tddl.rule.utils.ComparativeStringAnalyser;

/**
 * 兼容下tddl3.3版本之前的hint写法
 * 
 * <pre>
 * 1. /*+TDDL({type:executeByDB,dbId:tddl_group_0})*\
 * 2. /*+TDDL({type:executeByDBAndTab,tables:[moddbtab_0000,moddbtab_0001,],dbId:tddl_group_1,virtualTableName:moddbtab})*\/
 * 3. /*+TDDL({type:executeByCondition,parameters:["your_sharding_column=4;i"],virtualTableName:moddbtab})*\/
 * </pre>
 * 
 * @author jianghang 2014-3-12 上午11:17:56
 * @since 5.0.2
 */
public class OldHintParser {

    // ====================历史hint中的threadLocal key 禁用掉
    public static final String ROUTE_CONDITION = "ROUTE_CONDITION";
    public static final String IS_EXIST_QUITE  = "IS_EXIST_QUITE";
    public static final String DB_SELECTOR     = "DB_SELECTOR";
    public static final String RULE_SELECTOR   = "RULE_SELECTOR";

    public static void checkOldThreadLocalHint() {
        if (ThreadLocalMap.get(ROUTE_CONDITION) != null || ThreadLocalMap.get(IS_EXIST_QUITE) != null
            || ThreadLocalMap.get(DB_SELECTOR) != null || ThreadLocalMap.get(RULE_SELECTOR) != null) {
            throw new SqlParserException("tddl5.x版本已支持复杂SQL,因早期tddl版本不支持的SQL而被迫使用hint可以废弃了,所以以后不再支持此类的ThreadLocal hint,请尝试去除后直接运行SQL");
        }
    }

    /**
     * 解析老版本hint
     */
    public static RouteCondition extractHint(String tddlHint) {
        Map<String, Object> hints = simpleParser(tddlHint);
        RouteMethod route = RouteMethod.typeOf((String) hints.get("type"));
        RouteCondition condition = null;
        if (route == RouteMethod.executeByDBAndTab) {
            condition = new DirectlyRouteCondition();
            decodeDbId((DirectlyRouteCondition) condition, hints);
            decodeVirtualTableName((DirectlyRouteCondition) condition, hints);
            decodeTables((DirectlyRouteCondition) condition, hints);
        } else if (route == RouteMethod.executeByDBAndMutiReplace) {
            throw new OptimizerException("不再支持executeByDBAndMutiReplace,请使用executeByDBAndTab的表名逗号分隔配置为多表");
        } else if (route == RouteMethod.executeByDB) {
            condition = new DirectlyRouteCondition();
            decodeDbId((DirectlyRouteCondition) condition, hints);
        } else if (route == RouteMethod.executeByAdvancedCondition) {
            throw new OptimizerException("不再支持executeByAdvancedCondition");
        } else if (route == RouteMethod.executeByRule) {
            throw new OptimizerException("不再支持executeByRule");
        } else if (route == RouteMethod.executeByCondition) {
            condition = new RuleRouteCondition();
            decodeVirtualTableName(condition, hints);
            decodeRuleParameters((RuleRouteCondition) condition, hints);
            decodeSpecifyInfo(condition, hints);
        }

        return condition;
    }

    /**
     * DbId解码
     */
    private static void decodeDbId(DirectlyRouteCondition condition, Map<String, Object> hints) {
        String dbId = (String) hints.get("dbId");
        if (dbId == null) {
            throw new OptimizerException("hint contains no property 'dbId'.");
        }

        condition.setDBId(dbId);
    }

    /**
     * virtualTable解码
     */
    private static void decodeVirtualTableName(RouteCondition condition, Map<String, Object> hints) {
        String virtualTableName = (String) hints.get("virtualTableName");
        if (virtualTableName == null) {
            throw new OptimizerException("hint contains no property 'virtualTableName'.");
        }

        condition.setVirtualTableName(virtualTableName);
    }

    private static void decodeRuleParameters(RuleRouteCondition condition, Map<String, Object> hints) {
        List<String> parameters = (List<String>) hints.get("parameters");
        if (parameters == null) {
            throw new OptimizerException("hint contains no property 'parameters'.");
        }

        for (int i = 0; i < parameters.size(); i++) {
            String parameter = parameters.get(i);
            // 老版本的tddl中为 pk=1;int，而规则中的解析为pk=1:int，需要将;转换为:号
            parameter = StringUtils.replace(parameter, ";", ":");
            Map<String, Comparative> comparativeMap = ComparativeStringAnalyser.decodeComparativeString2Map(parameter);
            condition.getParameters().putAll(comparativeMap);
        }
    }

    /**
     * tables解码
     */
    private static void decodeTables(DirectlyRouteCondition condition, Map<String, Object> hints) {
        // modified by jiechen.qzm 确保一定有tables这个参数
        List<String> tables = (List<String>) hints.get("tables");
        if (tables == null) {
            throw new OptimizerException("hint contains no property 'tables'.");
        }

        // 设置table的Set<String>
        condition.setTables(new HashSet<String>(tables));
    }

    private static void decodeSpecifyInfo(RouteCondition condition, Map<String, Object> hints) throws JSONException {
        String skip = (String) hints.get("skip");
        String max = (String) hints.get("max");
        String orderby = (String) hints.get("orderby");
        if (skip != null || max != null || orderby != null) {
            throw new SqlParserException("不支持的tddl3.1.x的hint特殊参数");
        }
    }

    protected enum RouteMethod {
        executeByDBAndTab("executeByDBAndTab"), executeByDBAndMutiReplace("executeByDBAndMutiReplace"),
        executeByDB("executeByDB"), executeByRule("executeByRule"), executeByCondition("executeByCondition"),
        executeByAdvancedCondition("executeByAdvancedCondition");

        private String type;

        private RouteMethod(String type){
            this.type = type;
        }

        public String type() {
            return this.type;
        }

        public static RouteMethod typeOf(String type) {
            for (RouteMethod route : RouteMethod.values()) {
                if (route.type().equals(type)) {
                    return route;
                }
            }

            return null;
        }

    }

    private static Map<String, Object> simpleParser(String tddlHint) {
        Map<String, Object> result = new HashMap<String, Object>(3);
        boolean inArray = false;
        int start = 0;
        for (int i = 0; i < tddlHint.length(); i++) {
            char ch = tddlHint.charAt(i);
            switch (ch) {
                case '}':
                case ',':
                    if (!inArray) {
                        String part = tddlHint.substring(start, i);
                        String[] values = StringUtils.split(part, ':');
                        if (values.length != 2) {
                            throw new OptimizerException("hint syntax error : " + part);
                        }
                        String key = StringUtils.strip(values[0]);
                        String value = StringUtils.strip(values[1]);
                        if (value.charAt(0) == '[' && value.charAt(value.length() - 1) == ']') {
                            // 处理数组
                            value = value.substring(1, value.length() - 1);// 去掉前后[]
                            String[] vv = StringUtils.split(value, ',');
                            List<String> list = new ArrayList<String>();
                            for (String v : vv) {
                                v = StringUtils.remove(v, "\"");
                                v = StringUtils.remove(v, "\'");
                                list.add(v);
                            }
                            result.put(key, list);
                        } else {
                            if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
                                result.put(key, value.substring(1, value.length() - 1));// 去掉前后'号
                            } else {
                                result.put(key, value);
                            }
                        }
                        start = i + 1;// 跳到下一个
                    }
                    break;
                case '[':
                    inArray = true;
                    break;
                case ']':
                    inArray = false;
                    break;
                case '{':
                    start = i + 1;
                    break;

                default:
                    break;
            }
        }
        return result;
    }
}
