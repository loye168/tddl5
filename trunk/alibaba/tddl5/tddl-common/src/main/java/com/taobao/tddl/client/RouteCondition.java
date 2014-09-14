package com.taobao.tddl.client;

import java.util.Map;

/**
 * 别太纠结这混乱的包定义,一切都是为了以前老的客户端package定义
 * 
 * @author jianghang 2014-6-13 上午9:57:18
 * @since 5.1.0
 */
public interface RouteCondition {

    public enum ROUTE_TYPE {
        /** 连接关闭的时候清空 */
        FLUSH_ON_CLOSECONNECTION,
        /** 执行完成时就清空 */
        FLUSH_ON_EXECUTE;
    }

    public String getVirtualTableName();

    public void setVirtualTableName(String virtualTableName);

    public ROUTE_TYPE getRouteType();

    public void setRouteType(ROUTE_TYPE routeType);

    public Map<String, Object> getExtraCmds();
}
