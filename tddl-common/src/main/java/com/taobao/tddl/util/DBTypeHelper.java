package com.taobao.tddl.util;

import javax.sql.DataSource;

/**
 * 提供判断当前DataSource的类型, 主要为一些第三方工具来判断DBType <br/>
 * 别纠结,为了兼容tddl3保留的
 * 
 * @author jianghang 2014-6-26 下午6:11:36
 * @since 5.1.5
 */
public class DBTypeHelper {

    public static String getDbType(DataSource dataSource) {
        // TDDL5默认只支持mysql，所以直接返回true
        return "mysql";
    }

}
