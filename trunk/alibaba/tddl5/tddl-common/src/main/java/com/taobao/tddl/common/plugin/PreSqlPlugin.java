package com.taobao.tddl.common.plugin;

import java.sql.SQLException;

/**
 * 可以对用户传进来的sql进行一些预操作
 * 
 * @author mengshi.sunmengshi 2014年4月24日 上午10:48:32
 * @since 5.1.0
 */
public interface PreSqlPlugin {

    String handle(String orignSql) throws SQLException;

}
