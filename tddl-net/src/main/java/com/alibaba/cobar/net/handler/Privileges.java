package com.alibaba.cobar.net.handler;

import java.util.Set;

/**
 * 权限提供者
 * 
 * @author xianmao.hexm
 */
public interface Privileges {

    /**
     * 检查schema是否存在
     */
    boolean schemaExists(String schema);

    /**
     * 检查用户是否存在，并且可以使用host实行隔离策略。
     */
    boolean userExists(String user, String host);

    /**
     * 提供用户的服务器端密码
     */
    String getPassword(String user);

    /**
     * 提供有效的用户schema集合
     */
    Set<String> getUserSchemas(String user);

    /**
     * 判断是否是信任的白名单IP，可以没有配置信任白名单
     */
    boolean IsTrustedIp(String host);

}
