package com.taobao.tddl.common.model.hint;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.client.RouteCondition;
import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 扩展参数
 * 
 * @author jianghang 2014-1-13 下午6:08:14
 * @since 5.0.0
 */
public class ExtraCmdRouteCondition implements RouteCondition {

    protected String              virtualTableName;
    protected Map<String, Object> extraCmds = new HashMap<String, Object>();
    protected ROUTE_TYPE          routeType = ROUTE_TYPE.FLUSH_ON_EXECUTE;

    public String getVirtualTableName() {
        return virtualTableName;
    }

    public void setVirtualTableName(String virtualTableName) {
        this.virtualTableName = virtualTableName;
    }

    @Override
    public ROUTE_TYPE getRouteType() {
        return routeType;
    }

    @Override
    public void setRouteType(ROUTE_TYPE routeType) {
        this.routeType = routeType;
    }

    public Map<String, Object> getExtraCmds() {
        return extraCmds;
    }

    public void setExtraCmds(Map<String, Object> extraCmds) {
        this.extraCmds = extraCmds;
    }

    public void putExtraCmd(String key, Object value) {
        this.extraCmds.put(key, value);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
