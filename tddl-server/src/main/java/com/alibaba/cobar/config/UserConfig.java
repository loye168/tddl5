package com.alibaba.cobar.config;

import java.util.Set;

/**
 * 用户账户信息
 * 
 * @author xianmao.hexm 2011-1-11 下午02:26:09
 */
public class UserConfig {

    private String      name;
    private String      password;
    private Set<String> schemas; // 对应可访问的schema信息

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Set<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(Set<String> schemas) {
        this.schemas = schemas;
    }

}
