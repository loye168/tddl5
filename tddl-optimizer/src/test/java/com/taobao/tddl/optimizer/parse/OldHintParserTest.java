package com.taobao.tddl.optimizer.parse;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.common.model.hint.DirectlyRouteCondition;
import com.taobao.tddl.common.model.hint.RuleRouteCondition;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.parse.hint.SimpleHintParser;

/**
 * 兼容老规则测试
 * 
 * @author jianghang 2014-3-12 下午4:17:22
 * @since 5.0.2
 */
public class OldHintParserTest extends BaseOptimizerTest {

    @Test
    public void testExecuteByDB() {
        String sql = "/*+TDDL({type:executeByDB,dbId:xxx_group})*/delete from moddbtab_0000 where pk=?";
        DirectlyRouteCondition route = (DirectlyRouteCondition) SimpleHintParser.convertHint2RouteCondition(sql, null);

        Assert.assertEquals("xxx_group", route.getDbId());
        Assert.assertEquals(0, route.getTables().size());
    }

    @Test
    public void testExecuteByDBAndTab() {
        String sql = "/*+TDDL({type:executeByDBAndTab,tables:[real_tab_0,real_tab_1],dbId:xxx_group,virtualTableName:real_tab})*/delete from moddbtab where pk=?";
        DirectlyRouteCondition route = (DirectlyRouteCondition) SimpleHintParser.convertHint2RouteCondition(sql, null);

        Assert.assertEquals("xxx_group", route.getDbId());
        Assert.assertEquals("real_tab", route.getVirtualTableName());
        Assert.assertEquals(2, route.getTables().size());
    }

    @Test
    public void testExecuteByCondition() {
        String sql = "/*+TDDL({type:executeByCondition,parameters:[\"your_sharding_column=4;i\"],virtualTableName:real_tab})*/delete from moddbtab where pk=?";
        RuleRouteCondition route = (RuleRouteCondition) SimpleHintParser.convertHint2RouteCondition(sql, null);

        Assert.assertEquals("real_tab", route.getVirtualTableName());
        Assert.assertEquals("{YOUR_SHARDING_COLUMN=(=4)}", route.getParameters().toString());

        sql = "/*+TDDL({type:executeByCondition,parameters:[\"a=4;i or a=5;i\" , \"b>1;l and b<5;l\"],virtualTableName:real_tab})*/delete from moddbtab where pk=?";
        route = (RuleRouteCondition) SimpleHintParser.convertHint2RouteCondition(sql, null);

        Assert.assertEquals("real_tab", route.getVirtualTableName());
        Assert.assertEquals("{A=(=4) OR (=5), B=(>1) AND (<5)}", route.getParameters().toString());
    }

    @Test
    public void test_绑定变量测试() {
        String sql = "/*+TDDL({type:?,dbId:?})*/delete from moddbtab_0000 where pk=?";
        Map<Integer, ParameterContext> map = new HashMap<Integer, ParameterContext>();
        map.put(1, new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, "executeByDB" }));
        map.put(2, new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, "xxx_group" }));
        DirectlyRouteCondition route = (DirectlyRouteCondition) SimpleHintParser.convertHint2RouteCondition(sql, map);

        Assert.assertEquals("xxx_group", route.getDbId());
        Assert.assertEquals(0, route.getTables().size());
    }
}
