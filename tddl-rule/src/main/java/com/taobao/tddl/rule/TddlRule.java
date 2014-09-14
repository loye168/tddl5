package com.taobao.tddl.rule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;
import com.taobao.tddl.common.model.App;
import com.taobao.tddl.common.model.sqljep.Comparative;
import com.taobao.tddl.common.model.sqljep.ComparativeMapChoicer;
import com.taobao.tddl.monitor.logger.LoggerInit;
import com.taobao.tddl.rule.exception.RouteCompareDiffException;
import com.taobao.tddl.rule.model.Field;
import com.taobao.tddl.rule.model.MatcherResult;
import com.taobao.tddl.rule.model.TargetDB;
import com.taobao.tddl.rule.utils.ComparativeStringAnalyser;
import com.taobao.tddl.rule.utils.MatchResultCompare;

/**
 * 类名取名兼容老的rule代码<br/>
 * 结合tddl的动态规则管理体系，获取对应{@linkplain VirtualTableRule}
 * 规则定义，再根据sql中condition或者是setParam()提交的参数计算出路由规则 {@linkplain MatcherResult}
 * 
 * <pre>
 * condition简单语法： KEY CMP VALUE [:TYPE]
 * 1. KEY： 类似字段名字，用户随意定义
 * 2. CMP： 链接符，比如< = > 等，具体可查看{@linkplain Comparative}
 * 3. VALUE: 对应的值，比如1
 * 4. TYPE: 描述VALUE的类型，可选型，如果不填默认为Long类型。支持: int/long/string/date，可以使用首字母做为缩写，比如i/l/s/d。
 * 
 * 几个例子：
 * 1. id = 1
 * 2. id = 1 : long
 * 3. id > 1 and id < 1 : long
 * 4. gmt_create = 2011-11-11 : date
 * 5. id in (1,2,3,4) : long
 * </pre>
 * 
 * @author jianghang 2013-11-5 下午8:11:43
 * @since 5.0.0
 */
public class TddlRule extends TddlRuleConfig implements TddlTableRule {

    private VirtualTableRuleMatcher matcher  = new VirtualTableRuleMatcher();
    private List<App>               subApps;
    private List<TddlRule>          subRules = new ArrayList();
    private boolean                 subRule  = false;

    @Override
    public void doInit() {
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("TddlRule start init");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("appName is: " + this.appName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("unitName is: " + this.unitName);

        super.doInit();
        if (subApps != null) {
            for (App subApp : subApps) {
                TddlRule subRule = new TddlRule();
                subRule.setAppName(subApp.getAppName());
                subRule.setAppRuleFile(subApp.getRuleFile());
                subRule.setUnitName(unitName);
                subRule.setSubRule(true);
                try {
                    subRule.init();
                    subRules.add(subRule);
                } catch (TddlException e) {
                    logger.error("sub rule init error, app is :" + subApp, e);
                }
            }
        }
    }

    private void setSubRule(boolean b) {
        this.subRule = b;
    }

    @Override
    public MatcherResult route(String vtab, String condition) {
        return route(vtab, condition, super.getCurrentRule());
    }

    @Override
    public MatcherResult route(String vtab, String condition, String version) {
        return route(vtab, condition, super.getVersionRule(version));
    }

    @Override
    public MatcherResult route(String vtab, String condition, VirtualTableRoot specifyVtr) {
        return route(vtab, generateComparativeMapChoicer(condition), Lists.newArrayList(), specifyVtr);
    }

    @Override
    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args) {
        return route(vtab, choicer, args, super.getCurrentRule());
    }

    @Override
    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args, String version) {
        return route(vtab, choicer, args, super.getVersionRule(version));
    }

    @Override
    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args,
                               VirtualTableRoot specifyVtr) {
        return route(vtab, choicer, args, specifyVtr, false);
    }

    @Override
    public MatcherResult routeMverAndCompare(boolean isSelect, String vtab, ComparativeMapChoicer choicer,
                                             List<Object> args) throws RouteCompareDiffException {
        return routeMverAndCompare(isSelect, vtab, choicer, args, false);
    }

    @Override
    public MatcherResult routeMverAndCompare(boolean isSelect, String vtab, ComparativeMapChoicer choicer,
                                             List<Object> args, boolean forceAllowFullTableScan)
                                                                                                throws RouteCompareDiffException {
        if (super.getAllVersions().size() == 0) {
            if (allowEmptyRule) {
                return defaultRoute(vtab, null);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_ROUTE,
                    "routeWithMulVersion method just support multy version rule,use route method instead or config with multy version style!");
            }
        }

        // 如果只有单套规则,直接返回这套规则的路由结果
        if (super.getAllVersions().size() == 1) {
            return route(vtab, choicer, args, super.getCurrentRule(), forceAllowFullTableScan);
        }

        // 如果不止一套规则,那么计算两套规则,默认都返回新规则
        List<String> versions = super.getAllVersions();
        if (versions.size() != 2) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTE,
                "not support more than 2 copy rule compare,versions is:" + versions);
        }

        if (this.subRule || !this.subRules.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTE, "sub rule support 1 version only, app name is: "
                                                                + this.appName);
        }

        // 第一个排位的为旧规则
        MatcherResult oldResult = route(vtab, choicer, args, super.getCurrentRule(), forceAllowFullTableScan);
        if (isSelect) {
            return oldResult;
        } else {
            // 第二个排位的为新规则
            MatcherResult newResult = route(vtab,
                choicer,
                args,
                super.getVersionRule(super.getAllVersions().get(1)),
                forceAllowFullTableScan);
            boolean compareResult = MatchResultCompare.matchResultCompare(newResult, oldResult);
            if (compareResult) {
                return oldResult;
            } else {
                throw new RouteCompareDiffException();
            }
        }
    }

    /**
     * 返回对应表的defaultDbIndex
     */
    public String getDefaultDbIndex(String vtab) {
        return getDefaultDbIndex(vtab, super.getCurrentRule());
    }

    /**
     * 返回整个库的defaultDbIndex
     */
    public String getDefaultDbIndex() {
        return getDefaultDbIndex(null, super.getCurrentRule());
    }

    private MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args,
                                VirtualTableRoot specifyVtr, boolean forceAllowFullTableScan) {
        if (specifyVtr != null) {
            TableRule rule = specifyVtr.getVirtualTable(vtab);
            if (rule == null) {
                // 再尝试找一下子节点
                for (TddlRule subRule : this.subRules) {
                    rule = subRule.getTable(vtab);
                    if (rule != null) {
                        break;
                    }
                }
            }

            if (rule != null) {
                return matcher.match(choicer, args, rule, true, forceAllowFullTableScan);
            }
        }

        // 不存在规则，返回默认的
        return defaultRoute(vtab, specifyVtr);
    }

    // ================ helper method ================

    /**
     * 没有分库分表的逻辑表，返回指定库表
     * 
     * @param vtab
     * @param vtrCurrent
     * @return
     */
    private MatcherResult defaultRoute(String vtab, VirtualTableRoot vtrCurrent) {
        TargetDB targetDb = new TargetDB();
        // 设置默认的链接库，比如就是groupKey
        targetDb.setDbIndex(this.getDefaultDbIndex(vtab, vtrCurrent));
        // 设置表名，同名不做转化
        Map<String, Field> tableNames = new HashMap<String, Field>(1);
        tableNames.put(vtab, null);
        targetDb.setTableNames(tableNames);

        return new MatcherResult(Arrays.asList(targetDb),
            new HashMap<String, Comparative>(),
            new HashMap<String, Comparative>());
    }

    /**
     * 没有分库分表的逻辑表，先从dbIndex中获取映射的库，没有则返回默认的库
     * 
     * @param vtab
     * @param vtrCurrent
     * @return
     */
    private String getDefaultDbIndex(String vtab, VirtualTableRoot vtrCurrent) {
        if (vtrCurrent == null) {
            return super.defaultDbIndex;
        }

        if (vtab != null) {
            Map<String, String> dbIndexMap = vtrCurrent.getDbIndexMap();
            if (dbIndexMap != null && dbIndexMap.get(vtab) != null) {
                return dbIndexMap.get(vtab);
            }
        }
        String index = vtrCurrent.getDefaultDbIndex();
        if (index == null) {
            index = super.defaultDbIndex;
        }

        return index;
    }

    protected ComparativeMapChoicer generateComparativeMapChoicer(String condition) {
        Map<String, Comparative> comparativeMap = ComparativeStringAnalyser.decodeComparativeString2Map(condition);
        return new SimpleComparativeMapChoicer(comparativeMap);
    }

    class SimpleComparativeMapChoicer implements ComparativeMapChoicer {

        private Map<String, Comparative> comparativeMap = new HashMap<String, Comparative>();

        public SimpleComparativeMapChoicer(Map<String, Comparative> comparativeMap){
            this.comparativeMap = comparativeMap;
        }

        @Override
        public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> partnationSet) {
            return this.comparativeMap;
        }

        @Override
        public Comparative getColumnComparative(List<Object> arguments, String colName) {
            return this.comparativeMap.get(colName);
        }
    }

    public void setSubApps(List<App> subApps) {
        this.subApps = subApps;
    }

    public TableRule getTable(String tableName) {
        VirtualTableRoot vtr = super.getCurrentRule();
        TableRule rule = null;
        if (vtr != null) {
            rule = vtr.getVirtualTable(tableName);
        }

        if (rule == null) {
            for (TddlRule subRule : this.subRules) {
                rule = subRule.getTable(tableName);
                if (rule != null) {
                    return rule;
                }
            }
        }
        return rule;
    }

    /**
     * 获取所有的规则表
     */
    public List<TableRule> getTables() {
        List<TableRule> result = new ArrayList<TableRule>();
        VirtualTableRoot vrt = super.getCurrentRule();
        if (vrt == null) {
            return result;
        }

        Map<String, TableRule> tables = vrt.getTableRules();
        result.addAll(tables.values());
        for (TddlRule subRule : this.subRules) {
            VirtualTableRoot svrt = subRule.getCurrentRule();
            if (svrt != null) {
                tables = svrt.getTableRules();
                result.addAll(tables.values());
            }
        }
        return result;
    }

    public Map<String, String> getDbIndexMap() {
        Map<String, String> result = new HashMap<String, String>();
        VirtualTableRoot vrt = super.getCurrentRule();
        if (vrt == null) {
            return result;
        }
        Map<String, String> dbIndexMap = vrt.getDbIndexMap();
        if (dbIndexMap != null) {
            result.putAll(dbIndexMap);
        }
        for (TddlRule subRule : this.subRules) {
            VirtualTableRoot svrt = subRule.getCurrentRule();
            if (svrt != null) {
                dbIndexMap = svrt.getDbIndexMap();
                result.putAll(dbIndexMap);
            }
        }
        return result;
    }
}
